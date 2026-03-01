//! aiDB Server - Hybrid Vector Database
//!
//! Starts the gRPC server exposing the storage and indexing engine.
//! - Storage: Sled KV with Arrow RecordBatch for metadata + raw vectors
//! - Indexing: instant-distance for ANN/HNSW vector search
//! - Networking: Tonic + Tokio for robust async gRPC layer
//! - Future: DataFusion queries, Raft for distrib
//!
//! Usage:
//!   cargo run --bin load_data    # populate data
//!   cargo run --bin my_ai_db     # start server
//!   # Then query via gRPC (see README for grpcurl/curl-like examples)

use tonic::{transport::Server, Request, Response, Status};
// Axum + Tokio for REST API server (concurrent with gRPC on 11111)
use axum;
use std::net::SocketAddr;
use tokio::net::TcpListener;  // For Axum bind in 0.7+
// tower::ServiceBuilder unused (optional layers; keep dep for future)

// Core modules from lib (use package name for bin compatibility)
// Enables multi-model: Storage (Sled/JSON), Indexing (HNSW), Query (DataFusion SQL)
// + REST: Axum HTTP on port 11111 (concurrent with gRPC)
use my_ai_db::storage::{Storage, Document};
use my_ai_db::indexing::VectorIndex;
use my_ai_db::query::QueryEngine;
use my_ai_db::rest::create_router;  // REST router
use serde_json;  // For JSON in NoSQL insert_doc RPC
use my_ai_db::models::{User, Tenant, Environment, Collection, AuthPayload};
use my_ai_db::auth::{hash_password, verify_password, create_jwt, validate_jwt};

// Include generated proto code (from tonic-build on aidb package)
// Regenerates on build for new multi-model RPCs
pub mod aidb {
    tonic::include_proto!("aidb");
}

use aidb::{
    ai_db_service_server::{AiDbService, AiDbServiceServer},
    HybridRequest, HybridResponse, InsertDocRequest, InsertRequest, InsertResponse,
    SearchRequest, SearchResponse, SqlRequest, SqlResponse, VectorSearchRequest,
    RegisterRequest, RegisterResponse, LoginRequest, LoginResponse,
    CreateTenantRequest, CreateTenantResponse, CreateEnvironmentRequest, CreateEnvironmentResponse,
    CreateCollectionRequest, CreateCollectionResponse,
};

/// Service implementation for AiDbService
/// Combines multi-model engines:
/// - Storage: Sled (NoSQL JSON + vectors)
/// - Query: DataFusion for SQL on Arrow projection
/// - Indexing: HNSW for vector/hybrid
// Note: No Debug derive as Sled/Arrow types don't implement it (for tonic/server logging ok)
pub struct AiDbServiceImpl {
    // Unified storage layer (Sled for NoSQL/JSON/KV)
    storage: Storage,
}

impl AiDbServiceImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    fn check_auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<AuthPayload, Status> {
        let token = metadata.get("authorization")
            .ok_or(Status::unauthenticated("Missing token"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid token"))?;

        let token = if token.starts_with("Bearer ") { &token[7..] } else { token };
        validate_jwt(token).map_err(|_| Status::unauthenticated("Invalid token"))
    }
}

#[tonic::async_trait]
impl AiDbService for AiDbServiceImpl {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let hash = hash_password(&req.password).map_err(|_| Status::internal("Hash failed"))?;
        let user = User {
            username: req.username.clone(),
            password_hash: hash,
            tenants: vec![],
        };
        self.storage.create_user(user).map_err(|e| Status::already_exists(e.to_string()))?;
        Ok(Response::new(RegisterResponse { success: true }))
    }

    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();
        let user = self.storage.get_user(&req.username)
            .map_err(|_| Status::internal("DB error"))?
            .ok_or(Status::unauthenticated("User not found"))?;

        if !verify_password(&req.password, &user.password_hash).unwrap_or(false) {
            return Err(Status::unauthenticated("Invalid password"));
        }

        let token = create_jwt(&user.username).map_err(|_| Status::internal("Token gen failed"))?;
        Ok(Response::new(LoginResponse { token }))
    }

    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<CreateTenantResponse>, Status> {
        let claims = self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let tenant = Tenant {
            id: req.id.clone(),
            name: req.name,
            owner_id: claims.sub.clone(),
            environments: vec![],
        };
        self.storage.create_tenant(tenant).map_err(|e| Status::internal(e.to_string()))?;
        if let Some(mut user) = self.storage.get_user(&claims.sub).unwrap() {
             user.tenants.push(req.id);
             self.storage.update_user(user).unwrap();
        }
        Ok(Response::new(CreateTenantResponse { success: true }))
    }

    async fn create_environment(
        &self,
        request: Request<CreateEnvironmentRequest>,
    ) -> Result<Response<CreateEnvironmentResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let env = Environment {
            id: req.id.clone(),
            name: req.name,
            tenant_id: req.tenant_id.clone(),
            collections: vec![],
        };
        self.storage.create_environment(env).map_err(|e| Status::internal(e.to_string()))?;
        if let Some(mut tenant) = self.storage.get_tenant(&req.tenant_id).unwrap() {
             tenant.environments.push(req.id);
             self.storage.update_tenant(tenant).unwrap();
        }
        Ok(Response::new(CreateEnvironmentResponse { success: true }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let col = Collection {
            id: req.id.clone(),
            name: req.name,
            environment_id: req.env_id.clone(),
        };
        self.storage.create_collection(col).map_err(|e| Status::internal(e.to_string()))?;
        if let Some(mut env) = self.storage.get_environment(&req.env_id).unwrap() {
             env.collections.push(req.id);
             self.storage.update_environment(env).unwrap();
        }
        Ok(Response::new(CreateCollectionResponse { success: true }))
    }

    /// Insert: Writes an Arrow record (metadata) and vector to Sled by ID
    /// This fulfills the core storage requirement
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id;
        if collection_id.is_empty() { return Err(Status::invalid_argument("Missing collection_id")); }

        println!("📥 Insert request for ID: {}", req.id);

        let key = format!("{}/{}", collection_id, req.id);

        // Create Arrow RecordBatch metadata
        let metadata_batch = my_ai_db::storage::create_metadata_batch(&req.id, &req.text)
            .map_err(|e| Status::internal(format!("Arrow metadata error: {}", e)))?;

        // Store in Sled KV (separate trees for metadata/vectors)
        self.storage
            .insert(&key, metadata_batch, req.vector)
            .map_err(|e| Status::internal(format!("Sled storage error: {}", e)))?;

        // Index rebuild is on-search for simplicity (prod: incremental or persistent index)
        Ok(Response::new(InsertResponse { success: true }))
    }

    /// Search: Placeholder for text-based/hybrid search (to integrate DataFusion)
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        println!("🔍 Text search query: {}", req.query);
        // TODO: Implement robust querying with DataFusion over Arrow metadata
        // For now, stub response
        let results = vec![];
        Ok(Response::new(SearchResponse { results }))
    }

    /// VectorSearch: Core indexing engine - ANN search via HNSW
    /// Retrieves by similarity, returns IDs (metadata retrievable from storage)
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id;
        println!("🔍 Vector search (top_k={}): {:?}", req.top_k, req.query_vector);

        // Fetch vectors from storage
        let vectors = self.storage.get_vectors_in_collection(&collection_id)
            .map_err(|e| Status::internal(format!("Storage retrieval error: {}", e)))?;

        // Build index dynamically (advanced indexing library)
        let index = VectorIndex::build_from_vectors(vectors);

        // Perform search, get matching IDs
        let top_k = req.top_k as usize;
        let results = index.search(&req.query_vector, top_k);

        println!("✅ Found {} results via index", results.len());
        Ok(Response::new(SearchResponse { results }))
    }

    /// InsertDoc: NoSQL document insert using Serde JSON to Sled
    /// Unified storage for unstructured data; auto-projects to Arrow for SQL.
    async fn insert_doc(
        &self,
        request: Request<InsertDocRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id;
        if collection_id.is_empty() { return Err(Status::invalid_argument("Missing collection_id")); }
        println!("📥 InsertDoc (NoSQL/JSON) for ID: {}", req.id);

        // Parse flexible JSON metadata (NoSQL)
        let metadata_json: serde_json::Value = serde_json::from_str(&req.metadata_json)
            .unwrap_or(serde_json::json!({}));

        // Create Document for unified Sled storage
        let doc = Document {
            id: req.id.clone(),
            text: req.text,
            category: req.category,
            vector: req.vector,
            metadata: metadata_json,
        };

        // Insert to multi-model storage layer
        self.storage.insert_doc(doc, &collection_id)
            .map_err(|e| Status::internal(format!("NoSQL/JSON insert error: {}", e)))?;

        Ok(Response::new(InsertResponse { success: true }))
    }

    /// ExecuteSql: SQL queries via DataFusion on Arrow projection of NoSQL data
    /// Provides structured/relational access to JSON docs (e.g., filters, agg).
    async fn execute_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<Response<SqlResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id;
        println!("🔍 SQL query on multi-model data: {}", req.sql);

        // Init DataFusion engine (projects Sled JSON to Arrow table)
        let query_engine = QueryEngine::new(std::sync::Arc::new(self.storage.clone()), &collection_id).await
            .map_err(|e| Status::internal(format!("DataFusion init error: {}", e)))?;
        let results = query_engine.execute_sql(&req.sql).await
            .map_err(|e| Status::internal(format!("SQL execution error: {}", e)))?;

        // Serialize Arrow results to bytes (IPC stub for response)
        // Full impl: arrow::ipc::writer::FileWriter for vectorized transfer
        let mut arrow_buf: Vec<u8> = vec![];
        for batch in results {
            // Placeholder; preserves Arrow for client
            arrow_buf.extend(format!("{:?}", batch.schema()).as_bytes());
        }

        Ok(Response::new(SqlResponse { arrow_data: arrow_buf }))
    }

    /// HybridSearch: Custom planner for SQL + vector + NoSQL
    /// Routes predicate push-down: vector index first, then SQL filter on Arrow,
    /// full doc from Sled JSON. Max perf unified layer.
    async fn hybrid_search(
        &self,
        request: Request<HybridRequest>,
    ) -> Result<Response<HybridResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id;
        println!("🔍 HybridSearch: SQL='{}' + vector ANN (top_k={})", req.sql_filter, req.top_k);

        // Leverage hybrid planner (DataFusion SQL + HNSW + Sled NoSQL)
        let query_engine = QueryEngine::new(std::sync::Arc::new(self.storage.clone()), &collection_id).await
            .map_err(|e| Status::internal(format!("Planner error: {}", e)))?;
        let docs = query_engine.hybrid_query(&req.sql_filter, &req.query_vector, req.top_k as usize).await
            .map_err(|e| Status::internal(format!("Hybrid query error: {}", e)))?;

        // Results as IDs (extend to full JSON for NoSQL response)
        let results: Vec<String> = docs.iter().map(|(doc, _)| doc.id.clone()).collect();
        let cache_hits: Vec<bool> = docs.iter().map(|(_, from_cache)| *from_cache).collect();

        Ok(Response::new(HybridResponse { results, cache_hits }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // gRPC address (existing)
    let grpc_addr = "[::1]:50051".parse()?;
    // REST API address (new: port 11111 for curl/HTTP exposure of multi-model)
    let rest_addr: SocketAddr = "0.0.0.0:11111".parse()?;

    println!("🚀 aiDB Hybrid Multi-Model DB starting...");
    println!("📦 gRPC (Tonic) on {} | REST (Axum) on {}", grpc_addr, rest_addr);
    println!("📦 Storage Engine: Sled + Arrow/JSON (NoSQL)");
    println!("🔍 Indexing: instant-distance HNSW | SQL: DataFusion");
    println!("🌐 Networking: gRPC + REST/HTTP (Tokio concurrent)");
    println!("🧪 Run `cargo run --bin load_data` first");
    println!("📖 See README.md for gRPC/grpcurl + REST/cURL on :11111");

    // Init unified storage (shared between gRPC/REST)
    let storage = Storage::open("aidb_data")?;

    // gRPC service (multi-model: insert, vector, sql, hybrid)
    let grpc_service = AiDbServiceImpl::new(storage.clone());  // Clone for share (Sled thread-safe)

    // REST router (Axum: /insert_doc, /sql, /hybrid_search on :11111)
    let rest_app = create_router(storage);

    // Spawn REST server concurrently (Tokio task)
    // Axum 0.7: use TcpListener + axum::serve (avoids struct conflict with tonic Server)
    // _ to avoid unused warning; task runs independently
    let _rest_server = tokio::spawn(async move {
        let listener = TcpListener::bind(&rest_addr).await?;
        axum::serve(listener, rest_app.into_make_service()).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    // Run gRPC server (main task)
    Server::builder()
        .add_service(AiDbServiceServer::new(grpc_service))
        .serve(grpc_addr)
        .await?;

    // REST runs in background task (full shutdown signal for prod; gRPC main here)
    // Error in await?? type handled by task; REST server stays alive on 11111

    // Keep main alive for servers (in prod use select! or signal)
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}