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
use tracing::{info, warn, error, debug, instrument};

// Core modules from lib (use package name for bin compatibility)
// Enables multi-model: Storage (Sled/JSON), Indexing (HNSW), Query (DataFusion SQL)
// + REST: Axum HTTP on port 11111 (concurrent with gRPC)
use my_ai_db::storage::{Storage, Document};
use my_ai_db::query::QueryEngine;
use my_ai_db::rest::create_router;  // REST router
use serde_json;  // For JSON in NoSQL insert_doc RPC
use my_ai_db::tenants::{User, Tenant, Environment, Collection, AuthPayload};
use my_ai_db::auth::{hash_password, verify_password, create_jwt_with_session, validate_jwt};

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
    #[instrument(skip(self, request), fields(username))]
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        debug!(username = %req.username, "Register request received");
        
        let hash = hash_password(&req.password).map_err(|e| {
            error!(error = %e, "Password hashing failed");
            Status::internal("Hash failed")
        })?;
        
        let user = User {
            username: req.username.clone(),
            password_hash: hash,
            tenants: vec![],
        };
        
        self.storage.create_user(user).map_err(|e| {
            warn!(error = %e, username = %req.username, "User already exists");
            Status::already_exists(e.to_string())
        })?;
        
        info!(username = %req.username, "User registered successfully");
        Ok(Response::new(RegisterResponse { success: true }))
    }

    #[instrument(skip(self, request), fields(username))]
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();
        debug!(username = %req.username, "Login request received");
        
        let user = self.storage.get_user(&req.username)
            .map_err(|e| {
                error!(error = %e, username = %req.username, "Database error during login");
                Status::internal("DB error")
            })?
            .ok_or_else(|| {
                warn!(username = %req.username, "User not found");
                Status::unauthenticated("User not found")
            })?;

        if !verify_password(&req.password, &user.password_hash).unwrap_or(false) {
            warn!(username = %req.username, "Invalid password attempt");
            return Err(Status::unauthenticated("Invalid password"));
        }

        let (token, session_id) = create_jwt_with_session(&user.username).map_err(|e| {
            error!(error = %e, username = %user.username, "JWT creation failed");
            Status::internal("Token gen failed")
        })?;
        
        info!(username = %user.username, session_id = %session_id, "User logged in successfully");
        Ok(Response::new(LoginResponse { token, session_id }))
    }

    #[instrument(skip(self, request), fields(user_id, session_id))]
    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<CreateTenantResponse>, Status> {
        let claims = self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let session_id = claims.session_id.as_deref().unwrap_or("none");
        debug!(user_id = %claims.sub, session_id = %session_id, tenant_id = %req.id, "Create tenant request");
        
        let tenant = Tenant {
            id: req.id.clone(),
            name: req.name.clone(),
            owner_id: claims.sub.clone(),
            environments: vec![],
        };
        
        self.storage.create_tenant(tenant).map_err(|e| {
            error!(error = %e, session_id = %session_id, tenant_id = %req.id, "Failed to create tenant");
            Status::internal(e.to_string())
        })?;
        
        if let Some(mut user) = self.storage.get_user(&claims.sub).unwrap() {
             user.tenants.push(req.id.clone());
             self.storage.update_user(user).unwrap();
        }
        
        info!(session_id = %session_id, tenant_id = %req.id, name = %req.name, owner_id = %claims.sub, "Tenant created successfully");
        Ok(Response::new(CreateTenantResponse { success: true }))
    }

    #[instrument(skip(self, request), fields(session_id))]
    async fn create_environment(
        &self,
        request: Request<CreateEnvironmentRequest>,
    ) -> Result<Response<CreateEnvironmentResponse>, Status> {
        let claims = self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let session_id = claims.session_id.as_deref().unwrap_or("none");
        debug!(session_id = %session_id, env_id = %req.id, tenant_id = %req.tenant_id, "Create environment request");
        
        let env = Environment {
            id: req.id.clone(),
            name: req.name.clone(),
            tenant_id: req.tenant_id.clone(),
            collections: vec![],
        };
        
        self.storage.create_environment(env).map_err(|e| {
            error!(error = %e, session_id = %session_id, env_id = %req.id, "Failed to create environment");
            Status::internal(e.to_string())
        })?;
        
        if let Some(mut tenant) = self.storage.get_tenant(&req.tenant_id).unwrap() {
             tenant.environments.push(req.id.clone());
             self.storage.update_tenant(tenant).unwrap();
        }
        
        info!(session_id = %session_id, env_id = %req.id, tenant_id = %req.tenant_id, "Environment created successfully");
        Ok(Response::new(CreateEnvironmentResponse { success: true }))
    }

    #[instrument(skip(self, request), fields(session_id))]
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let claims = self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let session_id = claims.session_id.as_deref().unwrap_or("none");
        debug!(session_id = %session_id, collection_id = %req.id, env_id = %req.env_id, "Create collection request");
        
        let col = Collection {
            id: req.id.clone(),
            name: req.name.clone(),
            environment_id: req.env_id.clone(),
        };
        
        self.storage.create_collection(col).map_err(|e| {
            error!(error = %e, session_id = %session_id, collection_id = %req.id, "Failed to create collection");
            Status::internal(e.to_string())
        })?;
        
        if let Some(mut env) = self.storage.get_environment(&req.env_id).unwrap() {
             env.collections.push(req.id.clone());
             self.storage.update_environment(env).unwrap();
        }
        
        info!(session_id = %session_id, collection_id = %req.id, env_id = %req.env_id, "Collection created successfully");
        Ok(Response::new(CreateCollectionResponse { success: true }))
    }

    /// Insert: Writes an Arrow record (metadata) and vector to Sled by ID
    /// This fulfills the core storage requirement
    #[instrument(skip(self, request), fields(id, collection_id))]
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id.clone();
        if collection_id.is_empty() { 
            warn!("Insert request missing collection_id");
            return Err(Status::invalid_argument("Missing collection_id")); 
        }

        info!(id = %req.id, collection_id = %collection_id, "Insert request received");

        let key = format!("{}/{}", collection_id, req.id);

        // Create Arrow RecordBatch metadata
        let metadata_batch = my_ai_db::storage::create_metadata_batch(&req.id, &req.text)
            .map_err(|e| {
                error!(error = %e, id = %req.id, "Arrow metadata creation failed");
                Status::internal(format!("Arrow metadata error: {}", e))
            })?;

        // Store in Sled KV (separate trees for metadata/vectors)
        self.storage
            .insert(&key, metadata_batch, req.vector.clone())
            .map_err(|e| {
                error!(error = %e, key = %key, "Sled storage failed");
                Status::internal(format!("Sled storage error: {}", e))
            })?;

        info!(id = %req.id, collection_id = %collection_id, vector_len = req.vector.len(), "Insert completed successfully");
        Ok(Response::new(InsertResponse { success: true }))
    }

    /// Search: Placeholder for text-based/hybrid search (to integrate DataFusion)
    #[instrument(skip(self, request))]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        info!(query = %req.query, "Text search query received");
        // TODO: Implement robust querying with DataFusion over Arrow metadata
        // For now, stub response
        let results = vec![];
        Ok(Response::new(SearchResponse { results }))
    }

    /// VectorSearch: Core indexing engine - ANN search via HNSW
    /// Retrieves by similarity, returns IDs (metadata retrievable from storage)
    #[instrument(skip(self, request), fields(collection_id, top_k))]
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id.clone();
        debug!(collection_id = %collection_id, top_k = req.top_k, "Vector search request");

        let top_k = req.top_k as usize;
        let results = self
            .storage
            .vector_search(&collection_id, &req.query_vector, top_k)
            .map_err(|e| {
                error!(error = %e, collection_id = %collection_id, "Vector search failed");
                Status::internal(format!("Storage retrieval error: {}", e))
            })?;

        info!(collection_id = %collection_id, top_k = top_k, results_count = results.len(), "Vector search completed");
        Ok(Response::new(SearchResponse { results }))
    }

    /// InsertDoc: NoSQL document insert using Serde JSON to Sled
    /// Unified storage for unstructured data; auto-projects to Arrow for SQL.
    #[instrument(skip(self, request), fields(id, collection_id))]
    async fn insert_doc(
        &self,
        request: Request<InsertDocRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id.clone();
        if collection_id.is_empty() { 
            warn!("InsertDoc request missing collection_id");
            return Err(Status::invalid_argument("Missing collection_id")); 
        }
        
        info!(id = %req.id, collection_id = %collection_id, "InsertDoc request received");

        // Parse flexible JSON metadata (NoSQL)
        let metadata_json: serde_json::Value = serde_json::from_str(&req.metadata_json)
            .unwrap_or(serde_json::json!({}));

        // Create Document for unified Sled storage
        let doc = Document {
            id: req.id.clone(),
            text: req.text.clone(),
            category: req.category.clone(),
            vector: req.vector.clone(),
            metadata: metadata_json,
        };

        // Insert to multi-model storage layer
        self.storage.insert_doc(doc, &collection_id)
            .map_err(|e| {
                error!(error = %e, id = %req.id, collection_id = %collection_id, "NoSQL insert failed");
                Status::internal(format!("NoSQL/JSON insert error: {}", e))
            })?;

        info!(id = %req.id, collection_id = %collection_id, "InsertDoc completed successfully");
        Ok(Response::new(InsertResponse { success: true }))
    }

    /// ExecuteSql: SQL queries via DataFusion on Arrow projection of NoSQL data
    /// Provides structured/relational access to JSON docs (e.g., filters, agg).
    #[instrument(skip(self, request), fields(collection_id))]
    async fn execute_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<Response<SqlResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id.clone();
        info!(collection_id = %collection_id, sql = %req.sql, "SQL query request received");

        // Init DataFusion engine (projects Sled JSON to Arrow table)
        let query_engine = QueryEngine::new(std::sync::Arc::new(self.storage.clone()), &collection_id)
            .await
            .map_err(|e| {
                error!(error = %e, collection_id = %collection_id, "DataFusion initialization failed");
                Status::internal(format!("DataFusion init error: {}", e))
            })?;
        
        let results = query_engine.execute_sql(&req.sql).await
            .map_err(|e| {
                error!(error = %e, sql = %req.sql, "SQL execution failed");
                Status::internal(format!("SQL execution error: {}", e))
            })?;

        // Serialize Arrow results to bytes (IPC stub for response)
        let mut arrow_buf: Vec<u8> = vec![];
        for batch in results {
            arrow_buf.extend(format!("{:?}", batch.schema()).as_bytes());
        }

        info!(collection_id = %collection_id, sql = %req.sql, "SQL query completed");
        Ok(Response::new(SqlResponse { arrow_data: arrow_buf }))
    }

    /// HybridSearch: Custom planner for SQL + vector + NoSQL
    /// Routes predicate push-down: vector index first, then SQL filter on Arrow,
    /// full doc from Sled JSON. Max perf unified layer.
    #[instrument(skip(self, request), fields(collection_id, top_k))]
    async fn hybrid_search(
        &self,
        request: Request<HybridRequest>,
    ) -> Result<Response<HybridResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let collection_id = req.collection_id.clone();
        info!(collection_id = %collection_id, sql_filter = %req.sql_filter, top_k = req.top_k, "Hybrid search request");

        // Leverage hybrid planner (DataFusion SQL + HNSW + Sled NoSQL)
        let query_engine = QueryEngine::new(std::sync::Arc::new(self.storage.clone()), &collection_id)
            .await
            .map_err(|e| {
                error!(error = %e, collection_id = %collection_id, "Query engine initialization failed");
                Status::internal(format!("Planner error: {}", e))
            })?;
        
        let docs = query_engine.hybrid_query(&req.sql_filter, &req.query_vector, req.top_k as usize).await
            .map_err(|e| {
                error!(error = %e, collection_id = %collection_id, "Hybrid query failed");
                Status::internal(format!("Hybrid query error: {}", e))
            })?;

        // Results as IDs (extend to full JSON for NoSQL response)
        let results: Vec<String> = docs.iter().map(|(doc, _)| doc.id.clone()).collect();
        let cache_hits: Vec<bool> = docs.iter().map(|(_, from_cache)| *from_cache).collect();

        info!(collection_id = %collection_id, results_count = results.len(), cache_hits = ?cache_hits, "Hybrid search completed");
        Ok(Response::new(HybridResponse { results, cache_hits }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env file if present
    dotenvy::dotenv().ok();
    
    // Initialize logging (must be done early)
    let _guard = my_ai_db::logging::init_logging()?;
    
    // Read configuration from environment
    let grpc_port = std::env::var("AIDB_GRPC_PORT")
        .unwrap_or_else(|_| "50051".to_string());
    let rest_port = std::env::var("AIDB_REST_PORT")
        .unwrap_or_else(|_| "11111".to_string());
    let data_path = std::env::var("AIDB_DATA_PATH")
        .unwrap_or_else(|_| "aidb_data".to_string());
    
    // gRPC address
    let grpc_addr: SocketAddr = format!("[::1]:{}", grpc_port).parse()?;
    // REST API address
    let rest_addr: SocketAddr = format!("0.0.0.0:{}", rest_port).parse()?;

    info!("🚀 aiDB Hybrid Multi-Model DB starting...");
    info!(grpc_addr = %grpc_addr, rest_addr = %rest_addr, "Server addresses configured");
    info!("📦 Storage Engine: Sled + Arrow/JSON (NoSQL)");
    info!("🔍 Indexing: instant-distance HNSW | SQL: DataFusion");
    info!("🌐 Networking: gRPC + REST/HTTP (Tokio concurrent)");
    debug!("🧪 Run `cargo run --bin load_data` first");
    debug!("📖 See README.md for gRPC/grpcurl + REST/cURL");

    // Init unified storage (shared between gRPC/REST)
    let storage = Storage::open(&data_path)?;
    info!(data_path = %data_path, "Storage initialized");

    // gRPC service (multi-model: insert, vector, sql, hybrid)
    let grpc_service = AiDbServiceImpl::new(storage.clone());  // Clone for share (Sled thread-safe)

    // REST router (Axum: /insert_doc, /sql, /hybrid_search on :11111)
    let rest_app = create_router(storage);

    // Spawn REST server concurrently (Tokio task)
    let _rest_server = tokio::spawn(async move {
        let listener = TcpListener::bind(&rest_addr).await?;
        info!(rest_addr = %rest_addr, "REST server started");
        axum::serve(listener, rest_app.into_make_service()).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    // Run gRPC server (main task)
    info!(grpc_addr = %grpc_addr, "gRPC server starting");
    Server::builder()
        .add_service(AiDbServiceServer::new(grpc_service))
        .serve(grpc_addr)
        .await?;

    // Keep main alive for servers (in prod use select! or signal)
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}