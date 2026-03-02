//! Query Engine for multi-model DB
//!
//! Integrates Apache DataFusion for SQL on structured Arrow projections
//! from NoSQL JSON in Sled. Enables hybrid queries (SQL + vectors)
//! with predicate push-down for performance.
//! E.g., SELECT * FROM docs WHERE category='AI' AND vector similarity...

pub mod sql;
pub mod vector;

pub use sql::QueryEngine;

#[cfg(test)]
mod tests {
    use super::QueryEngine;
    use crate::storage::{Document, Storage};
    use std::fs;
    use serde_json;  // For json! in test doc

    #[tokio::test]
    async fn test_sql_and_hybrid_queries() -> Result<(), Box<dyn std::error::Error>> {
        // Temp storage for SQL/NoSQL test (multi-model; avoids lock races on shared DB)
        // Note: Uses dedicated temp path
        let temp_dir = std::env::temp_dir().join("aidb_test_query");
        let _ = fs::remove_dir_all(&temp_dir);
        let temp_path = temp_dir.to_str().unwrap();

        let storage = Storage::open(temp_path)?;

        // Insert sample multi-model doc for isolated test (NoSQL JSON + Arrow SQL)
        let doc = Document {
            id: "test_sql_doc".to_string(),
            text: "Test for SQL/DataFusion".to_string(),
            category: "AI".to_string(),
            vector: vec![1.0, 0.1, 0.1, 0.1],
            metadata: serde_json::json!({"test": true}),
        };
        storage.insert_doc(doc, "test_collection")?;

        // Init SQL engine (DataFusion on Arrow projection from NoSQL)
        let query_engine = QueryEngine::new(std::sync::Arc::new(storage), "test_collection").await?;

        // Test SQL query (structured relational on JSON data)
        let sql_results = query_engine.execute_sql("SELECT id, category FROM docs WHERE category = 'AI'")
            .await?;
        assert!(!sql_results.is_empty(), "SQL should return Arrow batches for push-down");

        // Test hybrid planner (vector ANN + SQL predicate)
        let hybrid_docs = query_engine.hybrid_query("category = 'AI'", &[1.0, 0.1, 0.1, 0.1], 1)
            .await?;
        assert!(!hybrid_docs.is_empty(), "Hybrid should return docs via push-down");

        let _ = fs::remove_dir_all(temp_dir);
        Ok(())
    }
}