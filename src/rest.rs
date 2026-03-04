//! REST API layer for aiDB using Axum (exposed on port 11111)
//!
//! Provides HTTP/JSON endpoints mirroring multi-model gRPC functionality:
//! - NoSQL inserts, SQL queries (DataFusion), hybrid search (SQL + vector + NoSQL).
//! - Enables easy curl access and web integration alongside gRPC.
//! - Shared state with Storage/QueryEngine for unified layer; Tokio-compatible.

use arrow::array::Array;
use axum::{
    extract::{Path, State},
    http::{StatusCode, Request, header},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post},
    Json, Router, Extension,
};
use serde::{Deserialize, Serialize};
use serde_json;  // For JSON parsing in NoSQL handler
use std::sync::Arc;
use tracing::{info, debug, warn, error, instrument};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::storage::{Document, Storage};
use crate::query::QueryEngine;
use crate::tenants::{User, Tenant, Environment, Collection, AuthPayload};
use crate::auth::{hash_password, verify_password, create_jwt_with_session, validate_jwt};
use crate::session::{get_session_manager, Session};
use crate::logging::{read_logs_by_session, JsonLogEntry};

/// Shared app state for REST handlers (Arc-wrapped for concurrency)
#[derive(Clone)]
pub struct AppState {
    storage: Arc<Storage>,
}

#[derive(Deserialize, ToSchema)]
pub struct UserRegister {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, ToSchema)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
    pub session_id: String,
}

/// DTO for NoSQL JSON insert (REST body)
#[derive(Deserialize, ToSchema, Clone)]
pub struct InsertDocRest {
    pub id: String,
    pub text: String,
    pub category: String,
    pub vector: Vec<f32>,
    pub metadata_json: String,  // Flexible NoSQL JSON
}

/// DTO for batch NoSQL JSON insert
#[derive(Deserialize, ToSchema)]
pub struct BatchInsertDocRest {
    pub documents: Vec<InsertDocRest>,
}

/// DTO for full-text search REST requests
#[derive(Deserialize, ToSchema)]
pub struct TextSearchRest {
    pub query: String,
    pub partial_match: bool,
    pub case_sensitive: bool,
    pub include_metadata: bool,
}

/// DTO for full-text search responses
#[derive(Serialize, ToSchema)]
pub struct TextSearchResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<DocumentSummary>,
}

#[derive(Serialize, ToSchema)]
pub struct DocumentSummary {
    pub id: String,
    pub text: String,
    pub category: String,
}

/// Generic REST response (JSON)
#[derive(Serialize, ToSchema)]
pub struct RestResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<String>,  // IDs or query results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hits: Option<Vec<bool>>, // True if fetched from cache
}

async fn auth_middleware(
    State(_state): State<Arc<AppState>>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header[7..];
    let claims = validate_jwt(token).map_err(|_| StatusCode::UNAUTHORIZED)?;

    // Touch session to update last activity
    if let Some(ref session_id) = claims.session_id {
        let session_manager = get_session_manager();
        session_manager.touch_session(session_id);
    }

    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}

#[derive(OpenApi)]
#[openapi(
    paths(
        register_handler,
        login_handler,
        insert_doc_handler,
        batch_insert_doc_handler,
        sql_handler,
        text_search_handler,
        hybrid_handler,
        health_handler
    ),
    components(
        schemas(UserRegister, UserLogin, LoginResponse, InsertDocRest, BatchInsertDocRest, TextSearchRest, TextSearchResponse, DocumentSummary, RestResponse, CreateTenantRest, CreateEnvRest, CreateCollectionRest, SqlRest, HybridRest)
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "aiDB", description = "aiDB REST API")
    )
)]
struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::HttpBuilder::new()
                        .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}

/// Create Axum router with multi-model endpoints
pub fn create_router(storage: Storage) -> Router {
    let state = Arc::new(AppState {
        storage: Arc::new(storage),
    });

    let auth_routes = Router::new()
        .route("/tenants", post(create_tenant_handler).get(get_tenants_handler))
        .route("/tenants/:tenant_id/environments", post(create_env_handler).get(get_envs_handler))
        .route("/environments/:env_id/collections", post(create_collection_handler).get(get_collections_handler))
        .route("/environments/:env_id/collections/:col_id", delete(delete_collection_handler))
        .route("/collections/:collection_id/docs", post(insert_doc_handler).put(update_doc_handler).get(list_docs_handler))
        .route("/collections/:collection_id/docs/batch", post(batch_insert_doc_handler))
        .route("/collections/:collection_id/docs/:doc_id", get(get_doc_handler).delete(delete_doc_handler))
        .route("/collections/:collection_id/sql", post(sql_handler))
        .route("/collections/:collection_id/search", post(text_search_handler))
        .route("/collections/:collection_id/hybrid", post(hybrid_handler))
        // RAG System endpoints
        .route("/collections/:collection_id/rag/ingest", post(rag_ingest_handler))
        .route("/collections/:collection_id/rag/search", post(rag_search_handler))
        .route("/collections/:collection_id/rag/docs", get(rag_list_docs_handler))
        .route("/collections/:collection_id/rag/docs/:doc_id", get(rag_get_doc_handler).delete(rag_delete_doc_handler))
        .route("/rag/embed", post(rag_embed_handler))
        // Session and logs endpoints
        .route("/sessions", get(get_sessions_handler))
        .route("/sessions/:session_id", get(get_session_handler))
        .route("/sessions/:session_id/logs", get(get_session_logs_handler))
        .route("/sessions/:session_id/logs/:level", get(get_session_logs_by_level_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/register", post(register_handler))
        .route("/login", post(login_handler))
        .route("/health", get(health_handler))
        .merge(auth_routes)
        .with_state(state)
}

/// Handler: User registration
#[utoipa::path(
    post,
    path = "/register",
    request_body = UserRegister,
    responses(
        (status = 200, description = "User registered successfully", body = RestResponse),
        (status = 400, description = "User already exists")
    )
)]
async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UserRegister>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(username = %payload.username, "REST register request");
    
    let hash = hash_password(&payload.password).map_err(|e| {
        error!(error = %e, "Password hashing failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    let user = User {
        username: payload.username.clone(),
        password_hash: hash,
        tenants: vec![],
    };
    
    state.storage.create_user(user).map_err(|e| {
        warn!(error = %e, username = %payload.username, "User registration failed");
        StatusCode::BAD_REQUEST
    })?;
    
    info!(username = %payload.username, "User registered via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "User registered".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: User login
#[utoipa::path(
    post,
    path = "/login",
    request_body = UserLogin,
    responses(
        (status = 200, description = "User logged in successfully", body = LoginResponse),
        (status = 401, description = "Unauthorized")
    )
)]
async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UserLogin>,
) -> Result<Json<LoginResponse>, StatusCode> {
    debug!(username = %payload.username, "REST login request");
    
    let user = state.storage.get_user(&payload.username)
        .map_err(|e| {
            error!(error = %e, "Database error during login");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or_else(|| {
            warn!(username = %payload.username, "User not found");
            StatusCode::UNAUTHORIZED
        })?;

    if !verify_password(&payload.password, &user.password_hash).unwrap_or(false) {
        warn!(username = %payload.username, "Invalid password attempt");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let (token, session_id) = create_jwt_with_session(&user.username).map_err(|e| {
        error!(error = %e, "JWT creation failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    info!(username = %user.username, session_id = %session_id, "User logged in via REST");
    Ok(Json(LoginResponse { token, session_id }))
}

#[derive(Deserialize, ToSchema)]
pub struct CreateTenantRest {
    pub id: String,
    pub name: String,
}

/// Handler: Create tenant
#[utoipa::path(
    post,
    path = "/tenants",
    request_body = CreateTenantRest,
    responses(
        (status = 200, description = "Tenant created successfully", body = RestResponse),
        (status = 401, description = "Unauthorized")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn create_tenant_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Json(payload): Json<CreateTenantRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(user_id = %claims.sub, tenant_id = %payload.id, "REST create tenant request");
    
    let tenant = Tenant {
        id: payload.id.clone(),
        name: payload.name.clone(),
        owner_id: claims.sub.clone(),
        environments: vec![],
    };
    state.storage.create_tenant(tenant).map_err(|e| {
        error!(error = %e, tenant_id = %payload.id, "Failed to create tenant");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    if let Some(mut user) = state.storage.get_user(&claims.sub).unwrap() {
        user.tenants.push(payload.id.clone());
        state.storage.update_user(user).unwrap();
    }

    info!(tenant_id = %payload.id, owner_id = %claims.sub, "Tenant created via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "Tenant created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

async fn get_tenants_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(user_id = %claims.sub, "REST get tenants request");
    
    let user = state.storage.get_user(&claims.sub).unwrap().unwrap();
    info!(user_id = %claims.sub, tenant_count = user.tenants.len(), "Tenants retrieved via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "User tenants".to_string(),
        results: user.tenants,
        cache_hits: None,
    }))
}

#[derive(Deserialize, ToSchema)]
pub struct CreateEnvRest {
    pub id: String,
    pub name: String,
}

async fn create_env_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Json(payload): Json<CreateEnvRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(tenant_id = %tenant_id, env_id = %payload.id, "REST create environment request");
    
    let env = Environment {
        id: payload.id.clone(),
        name: payload.name.clone(),
        tenant_id: tenant_id.clone(),
        collections: vec![],
    };
    state.storage.create_environment(env).map_err(|e| {
        error!(error = %e, env_id = %payload.id, "Failed to create environment");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    if let Some(mut tenant) = state.storage.get_tenant(&tenant_id).unwrap() {
        tenant.environments.push(payload.id.clone());
        state.storage.update_tenant(tenant).unwrap();
    }

    info!(env_id = %payload.id, tenant_id = %tenant_id, "Environment created via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "Environment created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

async fn get_envs_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(tenant_id = %tenant_id, "REST get environments request");
    
    let tenant = state.storage.get_tenant(&tenant_id).unwrap().unwrap();
    info!(tenant_id = %tenant_id, env_count = tenant.environments.len(), "Environments retrieved via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "Tenant environments".to_string(),
        results: tenant.environments,
        cache_hits: None,
    }))
}

#[derive(Deserialize, ToSchema)]
pub struct CreateCollectionRest {
    pub id: String,
    pub name: String,
}

async fn create_collection_handler(
    State(state): State<Arc<AppState>>,
    Path(env_id): Path<String>,
    Json(payload): Json<CreateCollectionRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(env_id = %env_id, collection_id = %payload.id, "REST create collection request");
    
    let col = Collection {
        id: payload.id.clone(),
        name: payload.name.clone(),
        environment_id: env_id.clone(),
    };
    state.storage.create_collection(col).map_err(|e| {
        error!(error = %e, collection_id = %payload.id, "Failed to create collection");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    if let Some(mut env) = state.storage.get_environment(&env_id).unwrap() {
        env.collections.push(payload.id.clone());
        state.storage.update_environment(env).unwrap();
    }

    info!(collection_id = %payload.id, env_id = %env_id, "Collection created via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "Collection created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

async fn get_collections_handler(
    State(state): State<Arc<AppState>>,
    Path(env_id): Path<String>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(env_id = %env_id, "REST get collections request");
    
    let env = state.storage.get_environment(&env_id).unwrap().unwrap();
    info!(env_id = %env_id, collection_count = env.collections.len(), "Collections retrieved via REST");
    Ok(Json(RestResponse {
        success: true,
        message: "Environment collections".to_string(),
        results: env.collections,
        cache_hits: None,
    }))
}

/// Handler: Insert NoSQL Document (JSON/Serde to Sled)
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/docs",
    request_body = InsertDocRest,
    responses(
        (status = 200, description = "Document inserted successfully", body = RestResponse),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("collection_id" = String, Path, description = "Collection ID")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn insert_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<InsertDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(collection_id = %collection_id, doc_id = %payload.id, "REST insert doc request");
    
    // Parse JSON metadata for NoSQL doc
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));

    let doc = Document {
        id: payload.id.clone(),
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    // Insert to unified storage
    if state.storage.insert_doc(doc, &collection_id).is_ok() {
        info!(collection_id = %collection_id, doc_id = %payload.id, "Document inserted via REST");
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL JSON doc inserted to Sled".to_string(),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        error!(collection_id = %collection_id, doc_id = %payload.id, "Failed to insert document");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: Batch Insert NoSQL Documents
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/docs/batch",
    request_body = BatchInsertDocRest,
    responses(
        (status = 200, description = "Batch of documents inserted successfully", body = RestResponse),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("collection_id" = String, Path, description = "Collection ID")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn batch_insert_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<BatchInsertDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(collection_id = %collection_id, count = payload.documents.len(), "REST batch insert doc request");
    
    let mut docs = Vec::new();
    for p in &payload.documents {
        let metadata_json: serde_json::Value = serde_json::from_str(&p.metadata_json)
            .unwrap_or(serde_json::json!({}));
        docs.push(Document {
            id: p.id.clone(),
            text: p.text.clone(),
            category: p.category.clone(),
            vector: p.vector.clone(),
            metadata: metadata_json,
        });
    }

    let payload_len = payload.documents.len();

    if state.storage.insert_docs(docs, &collection_id).is_ok() {
        info!(collection_id = %collection_id, count = payload_len, "Batch of documents inserted via REST");
        Ok(Json(RestResponse {
            success: true,
            message: format!("Batch of {} docs inserted", payload_len),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        error!(collection_id = %collection_id, "Failed to insert batch of documents");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: SQL query via DataFusion (on NoSQL Arrow projection)
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/sql",
    request_body = SqlRest,
    responses(
        (status = 200, description = "SQL query executed successfully", body = RestResponse),
        (status = 400, description = "Bad request")
    ),
    params(
        ("collection_id" = String, Path, description = "Collection ID")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn sql_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<SqlRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(collection_id = %collection_id, sql = %payload.sql, "REST SQL query request");

    // Init query engine (uses fixed project_to_arrow for compat)
    let query_engine = QueryEngine::new(state.storage.clone(), &collection_id)
        .await
        .map_err(|e| {
            error!(error = %e, collection_id = %collection_id, "DataFusion init failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Exec SQL ; catch DataFusion/Arrow errors (e.g., parse , empty , type mismatch)
    let results = query_engine.execute_sql(&payload.sql)
        .await
        .map_err(|e| {
            error!(error = %e, sql = %payload.sql, "SQL execution failed");
            StatusCode::BAD_REQUEST
        })?;

    // Extract IDs/results from Arrow batches (robust parse ; handles SELECT)
    let mut res_ids = vec![];
    for batch in results {
        if batch.num_rows() == 0 {
            continue;  // Skip empty
        }
        // Parse ID col (assumes first col ; extend for full row/UPDATE count)
        if let Some(id_col) = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..id_col.len() {
                res_ids.push(id_col.value(i).to_string());
            }
        }
    }
    
    info!(collection_id = %collection_id, sql = %payload.sql, row_count = res_ids.len(), "SQL query executed via REST");

    // Return full response (even for UPDATE/DELETE stub note ; ensures body)
    Ok(Json(RestResponse {
        success: true,
        message: format!("SQL executed: {} rows", res_ids.len()),
        results: res_ids,
        cache_hits: None,
    }))
}

/// Handler: Full-text search
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/search",
    request_body = TextSearchRest,
    responses(
        (status = 200, description = "Text search executed successfully", body = TextSearchResponse),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("collection_id" = String, Path, description = "Collection ID")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn text_search_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<TextSearchRest>,
) -> Result<Json<TextSearchResponse>, StatusCode> {
    let docs = state.storage.search_docs_text(
        &collection_id,
        &payload.query,
        payload.partial_match,
        payload.case_sensitive,
        payload.include_metadata,
    ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let results: Vec<DocumentSummary> = docs
        .into_iter()
        .map(|doc| DocumentSummary {
            id: doc.id,
            text: doc.text,
            category: doc.category,
        })
        .collect();

    Ok(Json(TextSearchResponse {
        success: true,
        message: format!("Text search matched {} documents", results.len()),
        results,
    }))
}


/// Handler: Hybrid search (SQL + vector via planner)
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/hybrid",
    request_body = HybridRest,
    responses(
        (status = 200, description = "Hybrid search completed successfully", body = RestResponse),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("collection_id" = String, Path, description = "Collection ID")
    ),
    security(
        ("bearerAuth" = [])
    )
)]
async fn hybrid_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<HybridRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(
        collection_id = %collection_id,
        sql_filter = %payload.sql_filter,
        top_k = payload.top_k,
        "REST hybrid search request"
    );
    
    // Use hybrid planner for push-down
    let query_engine = QueryEngine::new(state.storage.clone(), &collection_id)
        .await
        .map_err(|e| {
            error!(error = %e, collection_id = %collection_id, "Query engine init failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let docs: Vec<(Document, bool)> = query_engine.hybrid_query(&payload.sql_filter, &payload.query_vector, payload.top_k)
        .await
        .map_err(|e| {
            error!(error = %e, collection_id = %collection_id, "Hybrid query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let results: Vec<String> = docs.iter().map(|(doc, _)| doc.id.clone()).collect();
    let cache_hits: Vec<bool> = docs.iter().map(|(_, from_cache)| *from_cache).collect();
    
    info!(
        collection_id = %collection_id,
        results_count = results.len(),
        cache_hits_count = cache_hits.iter().filter(|&&h| h).count(),
        "Hybrid search completed via REST"
    );

    Ok(Json(RestResponse {
        success: true,
        message: format!("Hybrid search found {} docs", results.len()),
        results,
        cache_hits: Some(cache_hits),
    }))
}

/// DTO for hybrid REST
#[derive(Deserialize, ToSchema)]
pub struct HybridRest {
    pub sql_filter: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}

/// DTO for SQL REST
#[derive(Deserialize, ToSchema)]
pub struct SqlRest {
    pub sql: String,
}

/// Health check handler
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "aiDB REST API healthy", body = RestResponse)
    )
)]
async fn health_handler() -> Json<RestResponse> {
    debug!("REST health check");
    Json(RestResponse {
        success: true,
        message: "aiDB REST API healthy (multi-model on 11111)".to_string(),
        results: vec![],
        cache_hits: None,
    })
}

// --- Additional CRUD Handlers for NoSQL/REST (edit/update , delete) ---

/// DTO reuse for update (same as insert)
type UpdateDocRest = InsertDocRest;

/// Handler: Update/edit NoSQL doc (calls storage.update_doc for JSON upsert)
async fn update_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<UpdateDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(collection_id = %collection_id, doc_id = %payload.id, "REST update doc request");
    
    // Parse JSON , create/update Document
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));
    let doc = Document {
        id: payload.id.clone(),
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    if state.storage.update_doc(doc, &collection_id).is_ok() {
        info!(collection_id = %collection_id, doc_id = %payload.id, "Document updated via REST");
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL doc updated".to_string(),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        error!(collection_id = %collection_id, doc_id = %payload.id, "Failed to update document");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: Delete by ID (NoSQL + synced)
async fn delete_doc_handler(
    State(state): State<Arc<AppState>>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(collection_id = %collection_id, doc_id = %doc_id, "REST delete doc request");
    
    if state.storage.delete_doc(&collection_id, &doc_id).is_ok() {
        info!(collection_id = %collection_id, doc_id = %doc_id, "Document deleted via REST");
        Ok(Json(RestResponse {
            success: true,
            message: format!("Doc {} deleted", doc_id),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        warn!(collection_id = %collection_id, doc_id = %doc_id, "Document not found for deletion");
        Err(StatusCode::NOT_FOUND)
    }
}

async fn get_doc_handler(
    State(state): State<Arc<AppState>>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<Document>, StatusCode> {
    debug!(collection_id = %collection_id, doc_id = %doc_id, "REST get doc request");
    
    state.storage.get_doc(&collection_id, &doc_id)
        .map(|doc| {
            info!(collection_id = %collection_id, doc_id = %doc_id, "Document retrieved via REST");
            Json(doc)
        })
        .map_err(|e| {
            warn!(collection_id = %collection_id, doc_id = %doc_id, error = %e, "Document not found");
            StatusCode::NOT_FOUND
        })
}

async fn list_docs_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
) -> Result<Json<Vec<Document>>, StatusCode> {
    debug!(collection_id = %collection_id, "REST list docs request");
    
    state.storage.get_docs_in_collection(&collection_id)
        .map(|docs| {
            info!(collection_id = %collection_id, doc_count = docs.len(), "Documents listed via REST");
            Json(docs)
        })
        .map_err(|e| {
            error!(collection_id = %collection_id, error = %e, "Failed to list documents");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn delete_collection_handler(
    State(state): State<Arc<AppState>>,
    Path((env_id, col_id)): Path<(String, String)>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(env_id = %env_id, col_id = %col_id, "REST delete collection request");
    
    if state.storage.delete_collection(&env_id, &col_id).is_ok() {
        info!(env_id = %env_id, col_id = %col_id, "Collection deleted via REST");
        Ok(Json(RestResponse {
            success: true,
            message: format!("Collection {} deleted", col_id),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        error!(env_id = %env_id, col_id = %col_id, "Failed to delete collection");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

// --- Session and Logs Handlers ---

/// Response for listing sessions
#[derive(Serialize)]
pub struct SessionsResponse {
    pub sessions: Vec<Session>,
}

/// Response for session logs
#[derive(Serialize)]
pub struct SessionLogsResponse {
    pub session_id: String,
    pub logs: Vec<JsonLogEntry>,
}

/// Get all sessions for the current user
async fn get_sessions_handler(
    Extension(claims): Extension<AuthPayload>,
) -> Result<Json<SessionsResponse>, StatusCode> {
    debug!(username = %claims.sub, session_id = %claims.session_id.as_deref().unwrap_or("none"), "REST get sessions request");
    
    let session_manager = get_session_manager();
    let sessions = session_manager.get_user_sessions(&claims.sub);
    
    info!(username = %claims.sub, session_count = sessions.len(), "Sessions retrieved");
    Ok(Json(SessionsResponse { sessions }))
}

/// Get a specific session by ID
async fn get_session_handler(
    Extension(claims): Extension<AuthPayload>,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, StatusCode> {
    debug!(username = %claims.sub, session_id = %session_id, "REST get session request");
    
    let session_manager = get_session_manager();
    
    if let Some(session) = session_manager.get_session(&session_id) {
        // Verify the session belongs to the user
        if session.username != claims.sub {
            warn!(username = %claims.sub, session_id = %session_id, "Unauthorized session access attempt");
            return Err(StatusCode::FORBIDDEN);
        }
        
        info!(username = %claims.sub, session_id = %session_id, "Session retrieved");
        Ok(Json(session))
    } else {
        warn!(session_id = %session_id, "Session not found");
        Err(StatusCode::NOT_FOUND)
    }
}

/// Get all logs for a specific session (reads from JSON log file)
async fn get_session_logs_handler(
    Extension(claims): Extension<AuthPayload>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionLogsResponse>, StatusCode> {
    debug!(username = %claims.sub, session_id = %session_id, "REST get session logs request");
    
    let session_manager = get_session_manager();
    
    // Verify the session belongs to the user
    if let Some(session) = session_manager.get_session(&session_id) {
        if session.username != claims.sub {
            warn!(username = %claims.sub, session_id = %session_id, "Unauthorized session logs access attempt");
            return Err(StatusCode::FORBIDDEN);
        }
        
        // Read logs from JSON file
        let logs = read_logs_by_session(&session_id).unwrap_or_default();
        
        info!(username = %claims.sub, session_id = %session_id, log_count = logs.len(), "Session logs retrieved from file");
        Ok(Json(SessionLogsResponse { session_id, logs }))
    } else {
        warn!(session_id = %session_id, "Session not found for logs");
        Err(StatusCode::NOT_FOUND)
    }
}

/// Get logs for a specific session filtered by level (error, warn, info, debug)
async fn get_session_logs_by_level_handler(
    Extension(claims): Extension<AuthPayload>,
    Path((session_id, level)): Path<(String, String)>,
) -> Result<Json<SessionLogsResponse>, StatusCode> {
    debug!(username = %claims.sub, session_id = %session_id, level = %level, "REST get session logs by level request");
    
    let session_manager = get_session_manager();
    
    // Verify the session belongs to the user
    if let Some(session) = session_manager.get_session(&session_id) {
        if session.username != claims.sub {
            warn!(username = %claims.sub, session_id = %session_id, "Unauthorized session logs access attempt");
            return Err(StatusCode::FORBIDDEN);
        }
        
        // Read logs from JSON file and filter by level
        let all_logs = read_logs_by_session(&session_id).unwrap_or_default();
        let logs: Vec<JsonLogEntry> = all_logs
            .into_iter()
            .filter(|log| {
                log.level.as_ref()
                    .map(|l| l.eq_ignore_ascii_case(&level))
                    .unwrap_or(false)
            })
            .collect();
        
        info!(username = %claims.sub, session_id = %session_id, level = %level, log_count = logs.len(), "Session logs by level retrieved from file");
        Ok(Json(SessionLogsResponse { session_id, logs }))
    } else {
        warn!(session_id = %session_id, "Session not found for logs");
        Err(StatusCode::NOT_FOUND)
    }
}

// === RAG System REST Handlers ===

/// DTO for RAG text ingestion
#[derive(Deserialize)]
pub struct RagIngestRequest {
    /// Document ID
    pub doc_id: String,
    /// Text content to ingest
    pub text: String,
    /// Optional metadata as JSON string
    pub metadata_json: Option<String>,
    /// Optional source identifier
    pub source: Option<String>,
}

/// DTO for RAG search request
#[derive(Deserialize)]
pub struct RagSearchRequest {
    /// Search query text
    pub query: String,
    /// Number of results to return
    pub top_k: usize,
}

/// Response for RAG search
#[derive(Serialize)]
pub struct RagSearchResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<RagResultItem>,
}

/// Single result item in RAG search
#[derive(Serialize)]
pub struct RagResultItem {
    pub chunk_id: String,
    pub doc_id: String,
    pub text: String,
    pub score: f32,
    pub metadata: serde_json::Value,
}

/// Response for RAG ingestion
#[derive(Serialize)]
pub struct RagIngestResponse {
    pub success: bool,
    pub message: String,
    pub doc_id: String,
    pub chunks_created: usize,
}

/// Handler: Ingest text into RAG system
/// POST /collections/:collection_id/rag/ingest
#[instrument(skip(state, payload))]
pub async fn rag_ingest_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Path(collection_id): Path<String>,
    Json(payload): Json<RagIngestRequest>,
) -> Result<Json<RagIngestResponse>, StatusCode> {
    debug!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %payload.doc_id,
        text_len = payload.text.len(),
        "RAG ingest request"
    );
    
    // Parse metadata
    let metadata = payload.metadata_json
        .and_then(|m| serde_json::from_str(&m).ok())
        .unwrap_or(serde_json::json!({}));
    
    // Create RAG pipeline with simple embedder
    let pipeline = crate::rag::RagPipeline::simple()
        .map_err(|e| {
            error!(error = %e, "Failed to create RAG pipeline");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    // Ingest text
    let chunks = pipeline.ingest_text(
        &state.storage,
        &payload.text,
        &payload.doc_id,
        &collection_id,
        Some(metadata),
        payload.source,
    ).await.map_err(|e| {
        error!(error = %e, collection_id = %collection_id, doc_id = %payload.doc_id, "RAG ingestion failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    info!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %payload.doc_id,
        chunks_created = chunks.len(),
        "RAG ingestion completed"
    );
    
    Ok(Json(RagIngestResponse {
        success: true,
        message: format!("Ingested {} chunks", chunks.len()),
        doc_id: payload.doc_id,
        chunks_created: chunks.len(),
    }))
}

/// Handler: Search RAG documents
/// POST /collections/:collection_id/rag/search
#[instrument(skip(state, payload))]
pub async fn rag_search_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Path(collection_id): Path<String>,
    Json(payload): Json<RagSearchRequest>,
) -> Result<Json<RagSearchResponse>, StatusCode> {
    debug!(
        username = %claims.sub,
        collection_id = %collection_id,
        query_len = payload.query.len(),
        top_k = payload.top_k,
        "RAG search request"
    );
    
    // Create RAG pipeline
    let pipeline = crate::rag::RagPipeline::simple()
        .map_err(|e| {
            error!(error = %e, "Failed to create RAG pipeline");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    // Perform search
    let results = pipeline.search(
        &state.storage,
        &collection_id,
        &payload.query,
        payload.top_k,
    ).await.map_err(|e| {
        error!(error = %e, collection_id = %collection_id, "RAG search failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    // Convert results
    let result_items: Vec<RagResultItem> = results
        .into_iter()
        .map(|r| {
            let chunk_id = r.chunk.id;
            let doc_id = chunk_id
                .split('-')
                .next()
                .unwrap_or(chunk_id.as_str())
                .to_string();
            RagResultItem {
                chunk_id,
                doc_id,
                text: r.chunk.text,
                score: r.score,
                metadata: r.chunk.metadata,
            }
        })
        .collect();
    
    info!(
        username = %claims.sub,
        collection_id = %collection_id,
        results_count = result_items.len(),
        "RAG search completed"
    );
    
    Ok(Json(RagSearchResponse {
        success: true,
        message: format!("Found {} results", result_items.len()),
        results: result_items,
    }))
}

/// Handler: Get RAG document chunks
/// GET /collections/:collection_id/rag/docs/:doc_id
pub async fn rag_get_doc_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<Vec<crate::storage::RagStorageDocument>>, StatusCode> {
    debug!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %doc_id,
        "RAG get document request"
    );
    
    let chunks = state.storage.get_rag_doc_chunks(&collection_id, &doc_id)
        .map_err(|e| {
            warn!(error = %e, collection_id = %collection_id, doc_id = %doc_id, "Document not found");
            StatusCode::NOT_FOUND
        })?;
    
    info!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %doc_id,
        chunks = chunks.len(),
        "RAG document retrieved"
    );
    
    Ok(Json(chunks))
}

/// Handler: Delete RAG document
/// DELETE /collections/:collection_id/rag/docs/:doc_id
pub async fn rag_delete_doc_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<RestResponse>, StatusCode> {
    debug!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %doc_id,
        "RAG delete document request"
    );
    
    state.storage.delete_rag_doc(&collection_id, &doc_id)
        .map_err(|e| {
            error!(error = %e, collection_id = %collection_id, doc_id = %doc_id, "Failed to delete RAG document");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    info!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_id = %doc_id,
        "RAG document deleted"
    );
    
    Ok(Json(RestResponse {
        success: true,
        message: format!("RAG document {} deleted", doc_id),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: List all RAG documents in collection
/// GET /collections/:collection_id/rag/docs
pub async fn rag_list_docs_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Path(collection_id): Path<String>,
) -> Result<Json<Vec<String>>, StatusCode> {
    debug!(
        username = %claims.sub,
        collection_id = %collection_id,
        "RAG list documents request"
    );
    
    let doc_ids = state.storage.get_rag_doc_ids(&collection_id)
        .map_err(|e| {
            error!(error = %e, collection_id = %collection_id, "Failed to list RAG documents");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    info!(
        username = %claims.sub,
        collection_id = %collection_id,
        doc_count = doc_ids.len(),
        "RAG documents listed"
    );
    
    Ok(Json(doc_ids))
}

/// Handler: Generate embedding for text
/// POST /rag/embed
pub async fn rag_embed_handler(
    Extension(claims): Extension<AuthPayload>,
    Json(payload): Json<RagEmbedRequest>,
) -> Result<Json<RagEmbedResponse>, StatusCode> {
    debug!(
        username = %claims.sub,
        text_len = payload.text.len(),
        "RAG embed request"
    );
    
    // Create RAG pipeline
    let pipeline = crate::rag::RagPipeline::simple()
        .map_err(|e| {
            error!(error = %e, "Failed to create RAG pipeline");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    // Generate embedding
    let embedding = pipeline.embed(&payload.text)
        .map_err(|e| {
            error!(error = %e, "Failed to generate embedding");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    info!(
        username = %claims.sub,
        embedding_dim = embedding.len(),
        "Embedding generated"
    );
    
    Ok(Json(RagEmbedResponse {
        success: true,
        embedding,
        dimension: pipeline.embedding_dim(),
    }))
}

/// DTO for embedding request
#[derive(Deserialize)]
pub struct RagEmbedRequest {
    pub text: String,
}

/// Response for embedding
#[derive(Serialize)]
pub struct RagEmbedResponse {
    pub success: bool,
    pub embedding: Vec<f32>,
    pub dimension: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use std::fs;
    use tower::ServiceExt;  // For .oneshot() testing

    #[tokio::test]
    async fn test_rest_health_and_endpoints() {
        // Temp setup for REST test (avoids lock ; tests SQL fix)
        let temp_dir = std::env::temp_dir().join("aidb_test_rest");
        let _ = fs::remove_dir_all(&temp_dir);
        let temp_path = temp_dir.to_str().unwrap();
        let storage = Storage::open(temp_path).expect("Storage for REST test");

        // Insert sample for endpoint test (NoSQL + SQL projection)
        let doc = Document {
            id: "rest_test_doc".to_string(),
            text: "REST test".to_string(),
            category: "AI".to_string(),
            vector: vec![0.1, 0.1, 0.1, 0.1],
            metadata: serde_json::json!({"test": true}),
        };
        storage.insert_doc(doc).expect("Insert for test");

        // Create router
        let app = create_router(storage);

        // Test /health GET
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .method("GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("Health request");
        assert_eq!(response.status(), StatusCode::OK);

        // Test /sql POST (fixed: now returns response body ; SELECT on NoSQL)
        // Uses full body for test (addresses no-response issue)
        let sql_body = axum::body::Body::from(serde_json::to_string(&SqlRest {
            sql: "SELECT id, category FROM docs WHERE category = 'AI'".to_string(),
        }).unwrap());
        let sql_request = Request::builder()
            .uri("/sql")
            .method("POST")
            .header("content-type", "application/json")
            .body(sql_body)
            .unwrap();
        let sql_response = app
            .oneshot(sql_request)
            .await
            .expect("SQL request");
        assert_eq!(sql_response.status(), StatusCode::OK);  // Ensures body/response

        // Test /insert_doc POST (NoSQL) stub (extend reqwest for full)
        // ...

        let _ = fs::remove_dir_all(temp_dir);
    }
}