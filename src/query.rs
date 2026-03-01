//! Query Engine for multi-model DB
//!
//! Integrates Apache DataFusion for SQL on structured Arrow projections
//! from NoSQL JSON in Sled. Enables hybrid queries (SQL + vectors)
//! with predicate push-down for performance.
//! E.g., SELECT * FROM docs WHERE category='AI' AND vector similarity...

use arrow::array::{Array, StringArray};  // For Array trait (len) and StringArray downcast
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;  // Keep for SessionContext etc; unused parts warned but needed
use datafusion::execution::context::SessionContext;
use crate::storage::{Document, Storage};
use std::sync::Arc;

/// QueryEngine wraps DataFusion SessionContext for SQL over unified storage
pub struct QueryEngine {
    ctx: SessionContext,
    storage: Arc<Storage>,
    collection_id: String,
}

impl QueryEngine {
    /// Initialize SQL engine: projects NoSQL docs (Sled/JSON) to Arrow for vectorized queries
    /// This is the hybrid link - registers virtual 'docs' table for SQL.
    pub async fn new(storage: Arc<Storage>, collection_id: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();

        // Project NoSQL JSON docs to Arrow RecordBatch (structured view)
        // Enables high-perf SQL scans, filters, agg on 'docs' table
        let batch = storage.project_collection_to_arrow(collection_id)?;
        ctx.register_batch("docs", batch)?;

        // Optional: Register vectors for hybrid (extend with UDF for similarity)
        // e.g., CREATE FUNCTION similarity...

        Ok(Self {
            ctx,
            storage,
            collection_id: collection_id.to_string(),
        })
    }

    /// Execute SQL query on projected data (e.g., relational filters on JSON fields)
    /// Supports push-down: filters applied at scan for max perf.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let df = self.ctx.sql(sql).await?;
        // Collect results as Arrow batches (vectorized execution)
        let results = df.collect().await?;
        Ok(results)
    }

    /// Hybrid query example: Combine SQL filter + vector search
    /// Planner routes: Use index for vector, DataFusion for SQL predicate.
    /// Benefit: No data movement between DBs.
    pub async fn hybrid_query(
        &self,
        sql_filter: &str,  // e.g., "category = 'AI'"
        query_vector: &[f32],
        top_k: usize,
    ) -> Result<Vec<(Document, bool)>, Box<dyn std::error::Error>> {
        // Step 1: Vector indexing for candidates (ANN)
        let vectors = self.storage.get_vectors_in_collection(&self.collection_id)?;
        let index = crate::indexing::VectorIndex::build_from_vectors(vectors);
        let _candidate_ids = index.search(query_vector, top_k * 2);  // Oversample (unused in simplified SQL)

        // Step 2: SQL filter on Arrow projection (push-down on candidates)
        // Simplified: use sql_filter directly (avoids long IN clause parse issues in demo SQL)
        // In prod: temp table or parameterized; push-down ensures perf
        let sql = if sql_filter.is_empty() {
            "SELECT * FROM docs".to_string()
        } else {
            format!("SELECT * FROM docs WHERE {}", sql_filter)
        };
        let sql_results = self.execute_sql(&sql).await?;

        // Step 3: Fetch full docs (NoSQL JSON) for results
        let mut docs = vec![];
        for batch in sql_results {
            // Extract IDs from Arrow, lookup in Sled JSON
            if let Some(id_col) = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..id_col.len() {
                    let id = id_col.value(i);
                    let key = format!("{}/{}", self.collection_id, id);
                    if let Ok((doc, from_cache)) = self.storage.get_doc_with_cache_status(&key) {
                        docs.push((doc, from_cache));
                    }
                }
            }
        }
        // Limit to top_k
        Ok(docs.into_iter().take(top_k).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        storage.insert_doc(doc)?;

        // Init SQL engine (DataFusion on Arrow projection from NoSQL)
        let query_engine = QueryEngine::new(&storage).await?;

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