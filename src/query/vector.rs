use crate::indexing::VectorIndex;
use crate::storage::Storage;
use tracing::{info, debug, instrument};

impl Storage {
    /// Vector search helper to keep vector query logic in a dedicated module.
    #[instrument(skip(self, query_vector), fields(collection_id, top_k))]
    pub fn vector_search(&self, collection_id: &str, query_vector: &[f32], top_k: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        debug!(
            collection_id = %collection_id,
            top_k = top_k,
            vector_len = query_vector.len(),
            "Starting vector search"
        );
        
        let vectors = self.get_vectors_in_collection(collection_id)?;
        let index = VectorIndex::build_from_vectors(vectors);
        let results = index.search(query_vector, top_k);
        
        info!(
            collection_id = %collection_id,
            results_count = results.len(),
            "Vector search completed"
        );
        
        Ok(results)
    }
}
