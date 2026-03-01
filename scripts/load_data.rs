//! Load data script for aiDB multi-model DB
//!
//! Populates unified storage:
//! - NoSQL: JSON documents (Serde) in Sled
//! - Vectors/Metadata: For indexing
//! - SQL prep: Projects to Arrow for DataFusion
//! Run: cargo run --bin load_data
//! Enables hybrid queries (SQL + vector + JSON).

use my_ai_db::storage::{Document, Storage};
use my_ai_db::models::{User, Tenant, Environment, Collection};
use my_ai_db::auth::hash_password;
use my_ai_db::query::QueryEngine;
use serde_json::json;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open unified storage layer (Sled KV for multi-model)
    let storage = Storage::open("aidb_data")?;

    // Create default hierarchy
    let user = User {
        username: "admin".to_string(),
        password_hash: hash_password("admin").unwrap(),
        tenants: vec!["default_tenant".to_string()],
    };
    let _ = storage.create_user(user); // Ignore if exists

    let tenant = Tenant {
        id: "default_tenant".to_string(),
        name: "Default Tenant".to_string(),
        owner_id: "admin".to_string(),
        environments: vec!["default_env".to_string()],
    };
    let _ = storage.create_tenant(tenant);

    let env = Environment {
        id: "default_env".to_string(),
        name: "Default Env".to_string(),
        tenant_id: "default_tenant".to_string(),
        collections: vec!["default_collection".to_string()],
    };
    let _ = storage.create_environment(env);

    let col = Collection {
        id: "default_collection".to_string(),
        name: "Default Collection".to_string(),
        environment_id: "default_env".to_string(),
    };
    let _ = storage.create_collection(col);

    let collection_id = "default_collection";

    // Load sample multi-model data: 10 documents
    // NoSQL JSON for unstructured, with vector for ANN, fields for SQL
    // Simulates ingestion (e.g., from files/ML pipelines)
    for i in 0..10 {
        let id = format!("doc{}", i);
        let text = format!(
            "Sample document {}: hybrid multi-model DB covering SQL, NoSQL JSON, and vectors.",
            i
        );
        let category = if i % 2 == 0 { "AI" } else { "DB" };  // For SQL filters
        let mut vector = vec![0.1; 4];
        vector[i % 4] = 1.0;  // Varied for ANN testing

        // Flexible NoSQL metadata as JSON
        let metadata_json = json!({
            "source": "load_script",
            "tags": ["rust", "vector-db", if i % 2 == 0 { "ai" } else { "data" }],
            "timestamp": "2026-02-19"  // Simplified; chrono transitive but avoid dep
        });

        // Create Document and insert to unified Sled (NoSQL JSON + sync Arrow/vector)
        let doc = Document {
            id: id.clone(),
            text: text.clone(),
            category: category.to_string(),
            vector: vector.clone(),
            metadata: metadata_json,
        };
        storage.insert_doc(doc, collection_id)?;
    }

    println!("✅ Successfully loaded 10 multi-model documents (NoSQL JSON + vectors/Arrow) into aiDB");

    // Demo indexing engine
    let all_vectors = storage.get_vectors_in_collection(collection_id)?;
    let _index = my_ai_db::indexing::VectorIndex::build_from_vectors(all_vectors);
    println!("✅ Built HNSW index for vector search");

    // Demo SQL/DataFusion on projection + hybrid planner
    let query_engine = QueryEngine::new(Arc::new(storage.clone()), collection_id).await?;
    let sql_results = query_engine.execute_sql("SELECT id, category FROM docs WHERE category = 'AI'").await?;
    println!("✅ SQL query via DataFusion (on Arrow projection): {} AI docs found", sql_results.len());

    // Demo hybrid: SQL filter + vector
    let hybrid_docs = query_engine.hybrid_query("category = 'AI'", &[1.0, 0.1, 0.1, 0.1], 3).await?;
    println!("✅ Hybrid query (SQL push-down + ANN): {} results", hybrid_docs.len());

    Ok(())
}