use crate::indexing::VectorIndex;
use crate::storage::Storage;

impl Storage {
    /// Vector search helper to keep vector query logic in a dedicated module.
    pub fn vector_search(&self, collection_id: &str, query_vector: &[f32], top_k: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let vectors = self.get_vectors_in_collection(collection_id)?;
        let index = VectorIndex::build_from_vectors(vectors);
        Ok(index.search(query_vector, top_k))
    }
}
