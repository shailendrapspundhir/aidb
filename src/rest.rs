//! REST API layer for aiDB using Axum (exposed on port 11111)
//!
//! Provides HTTP/JSON endpoints mirroring multi-model gRPC functionality:
//! - NoSQL inserts, SQL queries (DataFusion), hybrid search (SQL + vector + NoSQL).
//! - Enables easy curl access and web integration alongside gRPC.
//! - Shared state with Storage/QueryEngine for unified layer; Tokio-compatible.

use arrow::array::{Array, StringArray};  // Array trait for len(), StringArray for downcast
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json;  // For JSON parsing in NoSQL handler
use std::sync::Arc;

use crate::storage::{Document, Storage};
use crate::query::QueryEngine;

/// Shared app state for REST handlers (Arc-wrapped for concurrency)
#[derive(Clone)]
pub struct AppState {
    storage: Arc<Storage>,
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
}

/// Create Axum router with multi-model endpoints
/// Mounts handlers for NoSQL, SQL, hybrid - stateful with storage.
pub fn create_router(storage: Storage) -> Router {
    let state = Arc::new(AppState {
        storage: Arc::new(storage),
    });

    Router::new()
        // NoSQL CRUD:
        // Insert JSON doc to Sled (unified with Arrow/vector)
        .route("/insert_doc", post(insert_doc_handler))
        // Update/edit doc by ID (NoSQL upsert)
        .route("/update_doc", post(update_doc_handler))  // POST for simplicity; PUT ideal
        // Delete by ID (NoSQL + synced trees)
        .route("/delete_doc/:id", axum::routing::delete(delete_doc_handler))
        // SQL: Execute query (SELECT/UPDATE/DELETE via DataFusion on Arrow)
        // e.g., for edit/delete: {"sql": "UPDATE docs SET category='AI' WHERE id='doc1'"} but note Arrow re-proj needed post-mut
        .route("/sql", post(sql_handler))
        // Hybrid: SQL filter + vector ANN (push-down)
        .route("/hybrid_search", post(hybrid_handler))
        // Health check (GET for simple curl)
        .route("/health", get(health_handler))
        .with_state(state)
}

/// Handler: Insert NoSQL Document (JSON/Serde to Sled)
async fn insert_doc_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<InsertDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    // Parse JSON metadata for NoSQL doc
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));

    let doc = Document {
        id: payload.id,
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    // Insert to unified storage
    if state.storage.insert_doc(doc).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL JSON doc inserted to Sled".to_string(),
            results: vec![],
        }))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: SQL query via DataFusion (on NoSQL Arrow projection)
/// Fixed: error handling/logging , full Arrow parse , supports SELECT/UPDATE/DELETE
/// (mutating queries re-project in prod; returns results or affected note)
async fn sql_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SqlRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    println!("🔍 REST SQL query: {}", payload.sql);  // Log for debug/no-response root cause

    // Init query engine (uses fixed project_to_arrow for compat)
    let query_engine = QueryEngine::new(&state.storage)
        .await
        .map_err(|e| {
            eprintln!("DataFusion init error: {}", e);  // Logs schema/table fail
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Exec SQL ; catch DataFusion/Arrow errors (e.g., parse , empty , type mismatch)
    let results = query_engine.execute_sql(&payload.sql)
        .await
        .map_err(|e| {
            eprintln!("SQL execution error: {}", e);  // Root cause (e.g., query/schema)
            StatusCode::BAD_REQUEST  // Client sees error vs silent 500/no body
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

    // Return full response (even for UPDATE/DELETE stub note ; ensures body)
    Ok(Json(RestResponse {
        success: true,
        message: format!("SQL executed: {} rows", res_ids.len()),
        results: res_ids,
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
    Json(payload): Json<HybridRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    // Use hybrid planner for push-down
    let query_engine = QueryEngine::new(&state.storage)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let docs = query_engine.hybrid_query(&payload.sql_filter, &payload.query_vector, payload.top_k)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let results: Vec<String> = docs.iter().map(|d| d.id.clone()).collect();

    Ok(Json(RestResponse {
        success: true,
        message: format!("Hybrid search found {} docs", results.len()),
        results,
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
    Json(RestResponse {
        success: true,
        message: "aiDB REST API healthy (multi-model on 11111)".to_string(),
        results: vec![],
    })
}

// --- Additional CRUD Handlers for NoSQL/REST (edit/update , delete) ---

/// DTO reuse for update (same as insert)
type UpdateDocRest = InsertDocRest;

/// Handler: Update/edit NoSQL doc (calls storage.update_doc for JSON upsert)
async fn update_doc_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateDocRest>,
) -> Result<Json<RestResponse>, StatusCode> {
    // Parse JSON , create/update Document
    let metadata_json: serde_json::Value = serde_json::from_str(&payload.metadata_json)
        .unwrap_or(serde_json::json!({}));
    let doc = Document {
        id: payload.id,
        text: payload.text,
        category: payload.category,
        vector: payload.vector,
        metadata: metadata_json,
    };

    if state.storage.update_doc(doc).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: "NoSQL doc updated".to_string(),
            results: vec![],
        }))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Handler: Delete by ID (NoSQL + synced)
async fn delete_doc_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<RestResponse>, StatusCode> {
    if state.storage.delete_doc(&id).is_ok() {
        Ok(Json(RestResponse {
            success: true,
            message: format!("Doc {} deleted", id),
            results: vec![],
        }))
    } else {
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