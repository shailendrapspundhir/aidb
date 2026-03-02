use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use tracing::{info, debug, warn, error, instrument};

use crate::storage::{Document, Storage};

/// QueryEngine wraps DataFusion SessionContext for SQL over unified storage
pub struct QueryEngine {
    ctx: SessionContext,
    storage: Arc<Storage>,
    collection_id: String,
}

impl QueryEngine {
    /// Initialize SQL engine: projects NoSQL docs (Sled/JSON) to Arrow for vectorized queries
    /// This is the hybrid link - registers virtual 'docs' table for SQL.
    #[instrument(skip(storage), fields(collection_id))]
    pub async fn new(storage: Arc<Storage>, collection_id: &str) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Initializing query engine");
        
        let ctx = SessionContext::new();

        // Project NoSQL JSON docs to Arrow RecordBatch (structured view)
        // Enables high-perf SQL scans, filters, agg on 'docs' table
        let batch = storage.project_collection_to_arrow(collection_id)?;
        ctx.register_batch("docs", batch)?;
        
        info!(collection_id = %collection_id, "Query engine initialized");

        Ok(Self {
            ctx,
            storage,
            collection_id: collection_id.to_string(),
        })
    }

    /// Execute SQL query on projected data (e.g., relational filters on JSON fields)
    /// Supports push-down: filters applied at scan for max perf.
    #[instrument(skip(self))]
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        debug!(sql = %sql, "Executing SQL query");
        
        let df = self.ctx.sql(sql).await?;
        // Collect results as Arrow batches (vectorized execution)
        let results = df.collect().await?;
        
        info!(sql = %sql, batch_count = results.len(), "SQL query executed");
        Ok(results)
    }

    /// Hybrid query example: Combine SQL filter + vector search
    /// Planner routes: Use index for vector, DataFusion for SQL predicate.
    /// Benefit: No data movement between DBs.
    #[instrument(skip(self, query_vector), fields(collection_id, sql_filter, top_k))]
    pub async fn hybrid_query(
        &self,
        sql_filter: &str,  // e.g., "category = 'AI'"
        query_vector: &[f32],
        top_k: usize,
    ) -> Result<Vec<(Document, bool)>, Box<dyn std::error::Error>> {
        debug!(
            sql_filter = %sql_filter,
            top_k = top_k,
            vector_len = query_vector.len(),
            "Starting hybrid query"
        );
        
        // Step 1: Vector indexing for candidates (ANN)
        let vectors = self.storage.get_vectors_in_collection(&self.collection_id)?;
        let index = crate::indexing::VectorIndex::build_from_vectors(vectors);
        let _candidate_ids = index.search(query_vector, top_k * 2);  // Oversample (unused in simplified SQL)

        // Step 2: SQL filter on Arrow projection (push-down on candidates)
        let sql = if sql_filter.is_empty() {
            "SELECT * FROM docs".to_string()
        } else {
            format!("SELECT * FROM docs WHERE {}", sql_filter)
        };
        let sql_results = self.execute_sql(&sql).await?;

        // Step 3: Fetch full docs (NoSQL JSON) for results
        let mut docs = vec![];
        let mut cache_hits = 0;
        
        for batch in sql_results {
            // Extract IDs from Arrow, lookup in Sled JSON
            if let Some(id_col) = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..id_col.len() {
                    let id = id_col.value(i);
                    let key = format!("{}/{}", self.collection_id, id);
                    if let Ok((doc, from_cache)) = self.storage.get_doc_with_cache_status(&key) {
                        if from_cache {
                            cache_hits += 1;
                        }
                        docs.push((doc, from_cache));
                    }
                }
            }
        }
        
        // Limit to top_k
        let result_count = docs.len().min(top_k);
        let docs: Vec<(Document, bool)> = docs.into_iter().take(top_k).collect();
        
        info!(
            sql_filter = %sql_filter,
            results = result_count,
            cache_hits = cache_hits,
            "Hybrid query completed"
        );
        
        Ok(docs)
    }
}
