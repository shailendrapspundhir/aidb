use instant_distance::{Builder, HnswMap, Point, Search};

#[derive(Clone, Debug)]
struct VectorPoint(Vec<f32>);

impl Point for VectorPoint {
    /// Euclidean (L2) distance for vector similarity search
    fn distance(&self, other: &Self) -> f32 {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }
}

/// VectorIndex wraps instant-distance HNSW for approximate nearest neighbor search
/// This provides the advanced indexing for the vector database
pub struct VectorIndex {
    map: HnswMap<VectorPoint, String>, // Maps points to IDs
}

impl VectorIndex {
    /// Build the index from a list of (id, vector) pairs obtained from storage
    pub fn build_from_vectors(vectors: Vec<(String, Vec<f32>)>) -> Self {
        let points: Vec<VectorPoint> = vectors
            .iter()
            .map(|(_, v)| VectorPoint(v.clone()))
            .collect();
        let values: Vec<String> = vectors.iter().map(|(id, _)| id.clone()).collect();

        let map = Builder::default().build(points, values);
        Self { map }
    }

    /// Search for k nearest neighbors by query vector, returns IDs
    /// This is the core indexing engine functionality
    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<String> {
        let query_point = VectorPoint(query_vector.to_vec());
        let mut search_state = Search::default();
        // Search returns iterator of (PointId, &Value), sorted by distance
        self.map
            .search(&query_point, &mut search_state)
            .take(k)
            .map(|item| item.value.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_indexing_and_search() {
        // Sample vectors for indexing
        let vectors = vec![
            ("doc1".to_string(), vec![1.0, 0.0, 0.0]),
            ("doc2".to_string(), vec![0.0, 1.0, 0.0]),
            ("doc3".to_string(), vec![0.0, 0.0, 1.0]),
        ];

        // Build index
        let index = VectorIndex::build_from_vectors(vectors);

        // Search with query close to doc1
        let query = vec![0.9, 0.1, 0.0];
        let results = index.search(&query, 1);

        // Should find nearest as doc1
        assert_eq!(results[0], "doc1");
        assert_eq!(results.len(), 1);
    }
}
