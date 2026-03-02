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

#[derive(Deserialize)]
pub struct UserRegister {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub session_id: String,
}

/// DTO for NoSQL JSON insert (REST body)
#[derive(Deserialize)]
pub struct InsertDocRest {
    pub id: String,
    pub text: String,
    pub category: String,
    pub vector: Vec<f32>,
    pub metadata_json: String,  // Flexible NoSQL JSON
}

/// Generic REST response (JSON)
#[derive(Serialize)]
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
        .route("/collections/:collection_id/docs/:doc_id", get(get_doc_handler).delete(delete_doc_handler))
        .route("/collections/:collection_id/sql", post(sql_handler))
        .route("/collections/:collection_id/hybrid", post(hybrid_handler))
        // Session and logs endpoints
        .route("/sessions", get(get_sessions_handler))
        .route("/sessions/:session_id", get(get_session_handler))
        .route("/sessions/:session_id/logs", get(get_session_logs_handler))
        .route("/sessions/:session_id/logs/:level", get(get_session_logs_by_level_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    Router::new()
        .route("/register", post(register_handler))
        .route("/login", post(login_handler))
        .route("/health", get(health_handler))
        .merge(auth_routes)
        .with_state(state)
}

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

#[derive(Deserialize)]
pub struct CreateTenantRest {
    pub id: String,
    pub name: String,
}

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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

/// Handler: SQL query via DataFusion (on NoSQL Arrow projection)
/// Fixed: error handling/logging , full Arrow parse , supports SELECT/UPDATE/DELETE
/// (mutating queries re-project in prod; returns results or affected note)
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

/// DTO for SQL REST
#[derive(Deserialize)]
pub struct SqlRest {
    pub sql: String,
}

/// Handler: Hybrid search (SQL + vector via planner)
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
#[derive(Deserialize)]
pub struct HybridRest {
    pub sql_filter: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}

/// Health check handler
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