use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json;
use std::sync::Arc;

use crate::auth::{create_jwt, hash_password, verify_password};
use crate::network::api::rest::models::{
    CreateCollectionRest, CreateEnvRest, CreateTenantRest, HybridRest, InsertDocRest,
    LoginResponse, RestResponse, SqlRest, UpdateDocRest, UserLogin, UserRegister,
};
use crate::network::api::rest::state::AppState;
use crate::query::QueryEngine;
use crate::storage::{Document, Storage};
use crate::tenants::{AuthPayload, Collection, Environment, Tenant, User};

/// Handler: User registration
pub async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UserRegister>,
) -> Result<Json<RestResponse>, StatusCode> {
    let hash = hash_password(&payload.password).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let user = User {
        username: payload.username.clone(),
        password_hash: hash,
        tenants: vec![],
    };
    state
        .storage
        .create_user(user)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(RestResponse {
        success: true,
        message: "User registered".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: User login
pub async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UserLogin>,
) -> Result<Json<LoginResponse>, StatusCode> {
    let user = state
        .storage
        .get_user(&payload.username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !verify_password(&payload.password, &user.password_hash).unwrap_or(false) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = create_jwt(&user.username).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(LoginResponse { token }))
}

/// Handler: Create tenant
pub async fn create_tenant_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
    Json(payload): Json<CreateTenantRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let tenant = Tenant {
        id: payload.id.clone(),
        name: payload.name,
        owner_id: claims.sub.clone(),
        environments: vec![],
    };
    state
        .storage
        .create_tenant(tenant)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(mut user) = state.storage.get_user(&claims.sub).unwrap() {
        user.tenants.push(payload.id);
        state.storage.update_user(user).unwrap();
    }

    Ok(Json(RestResponse {
        success: true,
        message: "Tenant created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: Get user tenants
pub async fn get_tenants_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<AuthPayload>,
) -> Result<Json<RestResponse>, StatusCode> {
    let user = state.storage.get_user(&claims.sub).unwrap().unwrap();
    Ok(Json(RestResponse {
        success: true,
        message: "User tenants".to_string(),
        results: user.tenants,
        cache_hits: None,
    }))
}

/// Handler: Create environment
pub async fn create_env_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Json(payload): Json<CreateEnvRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let env = Environment {
        id: payload.id.clone(),
        name: payload.name,
        tenant_id: tenant_id.clone(),
        collections: vec![],
    };
    state
        .storage
        .create_environment(env)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(mut tenant) = state.storage.get_tenant(&tenant_id).unwrap() {
        tenant.environments.push(payload.id);
        state.storage.update_tenant(tenant).unwrap();
    }

    Ok(Json(RestResponse {
        success: true,
        message: "Environment created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: Get tenant environments
pub async fn get_envs_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> Result<Json<RestResponse>, StatusCode> {
    let tenant = state.storage.get_tenant(&tenant_id).unwrap().unwrap();
    Ok(Json(RestResponse {
        success: true,
        message: "Tenant environments".to_string(),
        results: tenant.environments,
        cache_hits: None,
    }))
}

/// Handler: Create collection
pub async fn create_collection_handler(
    State(state): State<Arc<AppState>>,
    Path(env_id): Path<String>,
    Json(payload): Json<CreateCollectionRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let col = Collection {
        id: payload.id.clone(),
        name: payload.name,
        environment_id: env_id.clone(),
    };
    state
        .storage
        .create_collection(col)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(mut env) = state.storage.get_environment(&env_id).unwrap() {
        env.collections.push(payload.id);
        state.storage.update_environment(env).unwrap();
    }

    Ok(Json(RestResponse {
        success: true,
        message: "Collection created".to_string(),
        results: vec![],
        cache_hits: None,
    }))
}

/// Handler: Get environment collections
pub async fn get_collections_handler(
    State(state): State<Arc<AppState>>,
    Path(env_id): Path<String>,
) -> Result<Json<RestResponse>, StatusCode> {
    let env = state.storage.get_environment(&env_id).unwrap().unwrap();
    Ok(Json(RestResponse {
        success: true,
        message: "Environment collections".to_string(),
        results: env.collections,
        cache_hits: None,
    }))
}

/// Handler: Insert NoSQL Document (JSON/Serde to Sled)
pub async fn insert_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<InsertDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));

    let doc = Document {
        id: payload.id,
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    if state.storage.insert_doc(doc, &collection_id).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL JSON doc inserted to Sled".to_string(),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: SQL query via DataFusion
pub async fn sql_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<SqlRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    println!("🔍 REST SQL query: {}", payload.sql);

    let query_engine = QueryEngine::new(state.storage.clone(), &collection_id)
        .await
        .map_err(|e| {
            eprintln!("DataFusion init error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let results = query_engine
        .execute_sql(&payload.sql)
        .await
        .map_err(|e| {
            eprintln!("SQL execution error: {}", e);
            StatusCode::BAD_REQUEST
        })?;

    let mut res_ids = vec![];
    for batch in results {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(id_col) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            for i in 0..id_col.len() {
                res_ids.push(id_col.value(i).to_string());
            }
        }
    }

    Ok(Json(RestResponse {
        success: true,
        message: format!("SQL executed: {} rows", res_ids.len()),
        results: res_ids,
        cache_hits: None,
    }))
}

/// Handler: Hybrid search (SQL + vector via planner)
pub async fn hybrid_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<HybridRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let query_engine = QueryEngine::new(state.storage.clone(), &collection_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let docs: Vec<(Document, bool)> = query_engine
        .hybrid_query(&payload.sql_filter, &payload.query_vector, payload.top_k)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let results: Vec<String> = docs.iter().map(|(doc, _)| doc.id.clone()).collect();
    let cache_hits: Vec<bool> = docs.iter().map(|(_, from_cache)| *from_cache).collect();

    Ok(Json(RestResponse {
        success: true,
        message: format!("Hybrid search found {} docs", results.len()),
        results,
        cache_hits: Some(cache_hits),
    }))
}

/// Handler: Health check
pub async fn health_handler() -> Json<RestResponse> {
    Json(RestResponse {
        success: true,
        message: "aiDB REST API healthy (multi-model on 11111)".to_string(),
        results: vec![],
        cache_hits: None,
    })
}

/// Handler: Update/edit NoSQL doc
pub async fn update_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
    Json(payload): Json<UpdateDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));
    let doc = Document {
        id: payload.id,
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    if state.storage.update_doc(doc, &collection_id).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL doc updated".to_string(),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: Delete doc by ID
pub async fn delete_doc_handler(
    State(state): State<Arc<AppState>>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<RestResponse>, StatusCode> {
    if state.storage.delete_doc(&collection_id, &doc_id).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: format!("Doc {} deleted", doc_id),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// Handler: Get doc by ID
pub async fn get_doc_handler(
    State(state): State<Arc<AppState>>,
    Path((collection_id, doc_id)): Path<(String, String)>,
) -> Result<Json<Document>, StatusCode> {
    state
        .storage
        .get_doc(&collection_id, &doc_id)
        .map(Json)
        .map_err(|_| StatusCode::NOT_FOUND)
}

/// Handler: List all docs in collection
pub async fn list_docs_handler(
    State(state): State<Arc<AppState>>,
    Path(collection_id): Path<String>,
) -> Result<Json<Vec<Document>>, StatusCode> {
    state
        .storage
        .get_docs_in_collection(&collection_id)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Handler: Delete collection
pub async fn delete_collection_handler(
    State(state): State<Arc<AppState>>,
    Path((env_id, col_id)): Path<(String, String)>,
) -> Result<Json<RestResponse>, StatusCode> {
    if state.storage.delete_collection(&env_id, &col_id).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: format!("Collection {} deleted", col_id),
            results: vec![],
            cache_hits: None,
        }))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}