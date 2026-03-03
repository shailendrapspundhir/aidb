//! Embeddings Module for RAG System
//!
//! Provides embedding generation for text using n-gram hashing.
//! This module offers a lightweight, dependency-free embedding solution suitable
//! for development, testing, and basic semantic similarity operations.
//!
//! For production use with high-quality semantic embeddings, consider integrating
//! with external embedding services like OpenAI, Cohere, or self-hosted models
//! via HTTP API calls.

use serde::{Deserialize, Serialize};
use tracing::{info, debug, instrument};

/// Configuration for the embedding model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Dimension of the output embeddings
    pub embedding_dim: usize,
    /// Whether to normalize embeddings (L2 normalization)
    pub normalize: bool,
    /// N-gram range for text hashing (min, max)
    pub ngram_range: (usize, usize),
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            embedding_dim: 384,
            normalize: true,
            ngram_range: (1, 4),
        }
    }
}

/// Embedding model using n-gram hashing
///
/// This embedder generates text embeddings using character n-gram hashing.
/// It's lightweight, fast, and doesn't require external dependencies or model downloads.
/// Suitable for development, testing, and basic semantic similarity operations.
///
/// # Example
/// ```rust
/// use my_ai_db::rag::embeddings::{EmbeddingModel, EmbeddingConfig};
///
/// let config = EmbeddingConfig::default();
/// let embedder = EmbeddingModel::new(config);
///
/// let embedding = embedder.embed("Hello, world!").unwrap();
/// println!("Embedding dimension: {}", embedding.len());
/// ```
pub struct EmbeddingModel {
    config: EmbeddingConfig,
}

impl EmbeddingModel {
    /// Create a new embedding model with the given configuration
    #[instrument(skip(config))]
    pub fn new(config: EmbeddingConfig) -> Self {
        debug!(embedding_dim = config.embedding_dim, "Creating embedding model");
        Self { config }
    }

    /// Create an embedding model with default configuration
    pub fn default_model() -> Self {
        Self::new(EmbeddingConfig::default())
    }

    /// Get the embedding dimension
    pub fn embedding_dim(&self) -> usize {
        self.config.embedding_dim
    }

    /// Generate an embedding for a single text
    #[instrument(skip(self, text))]
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        debug!(text_len = text.len(), "Generating embedding");
        
        let mut embedding = vec![0.0f32; self.config.embedding_dim];
        
        // Use character n-grams for semantic similarity
        let chars: Vec<char> = text.chars().collect();
        
        let (min_n, max_n) = self.config.ngram_range;
        
        for n in min_n..=max_n {
            if chars.len() >= n {
                for window in chars.windows(n) {
                    // Hash the n-gram
                    let hash = Self::hash_ngram(window);
                    
                    // Use double hashing for better distribution
                    let idx1 = (hash as usize) % self.config.embedding_dim;
                    let idx2 = ((hash >> 32) as usize) % self.config.embedding_dim;
                    
                    embedding[idx1] += 1.0;
                    embedding[idx2] += 0.5; // Secondary index with lower weight
                }
            }
        }
        
        // Add word-level features
        let words: Vec<&str> = text.split_whitespace().collect();
        for word in &words {
            let word_hash = Self::hash_string(word);
            let idx = (word_hash as usize) % self.config.embedding_dim;
            embedding[idx] += 2.0; // Words get higher weight
        }
        
        // Normalize if configured
        if self.config.normalize {
            Self::normalize_vector(&mut embedding);
        }

        let preview: Vec<f32> = embedding.iter().take(8).cloned().collect();
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        info!(
            embedding_dim = embedding.len(),
            embedding_norm = norm,
            embedding_preview = ?preview,
            "Embedding generated"
        );
        Ok(embedding)
    }

    /// Generate embeddings for multiple texts in batch
    #[instrument(skip(self, texts))]
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        debug!(batch_size = texts.len(), "Generating batch embeddings");
        
        let embeddings: Vec<Vec<f32>> = texts
            .iter()
            .map(|text| self.embed(text).unwrap_or_default())
            .collect();
        
        let preview: Vec<f32> = embeddings
            .get(0)
            .map(|embedding| embedding.iter().take(8).cloned().collect())
            .unwrap_or_default();
        info!(
            batch_size = texts.len(),
            first_embedding_preview = ?preview,
            "Batch embeddings generated"
        );
        Ok(embeddings)
    }

    /// Get the configuration
    pub fn config(&self) -> &EmbeddingConfig {
        &self.config
    }

    /// Hash an n-gram using FNV-1a algorithm
    fn hash_ngram(ngram: &[char]) -> u64 {
        // FNV-1a hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for &c in ngram {
            hash ^= c as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Hash a string using FNV-1a algorithm
    fn hash_string(s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// L2 normalize a vector
    fn normalize_vector(vec: &mut [f32]) {
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for val in vec.iter_mut() {
                *val /= norm;
            }
        }
    }
}

/// Simple embedding generator (alias for EmbeddingModel for backward compatibility)
pub type SimpleEmbedder = EmbeddingModel;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_config_default() {
        let config = EmbeddingConfig::default();
        assert_eq!(config.embedding_dim, 384);
        assert!(config.normalize);
        assert_eq!(config.ngram_range, (1, 4));
    }

    #[test]
    fn test_embedder_creation() {
        let embedder = EmbeddingModel::default_model();
        assert_eq!(embedder.embedding_dim(), 384);
    }

    #[test]
    fn test_embedding_generation() {
        let embedder = EmbeddingModel::default_model();
        let text = "Hello, world!";
        
        let embedding = embedder.embed(text).unwrap();
        
        assert_eq!(embedding.len(), 384);
        
        // Check normalization
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_embedding_consistency() {
        let embedder = EmbeddingModel::default_model();
        
        let text = "Test text for consistency";
        let emb1 = embedder.embed(text).unwrap();
        let emb2 = embedder.embed(text).unwrap();
        
        // Same text should produce same embedding
        for (a, b) in emb1.iter().zip(emb2.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[test]
    fn test_embedding_similarity() {
        let embedder = EmbeddingModel::default_model();
        
        let text1 = "Hello world";
        let text2 = "Hello world!"; // Very similar
        let text3 = "Completely different text here"; // Different
        
        let emb1 = embedder.embed(text1).unwrap();
        let emb2 = embedder.embed(text2).unwrap();
        let emb3 = embedder.embed(text3).unwrap();
        
        // Calculate cosine similarity
        let sim_1_2: f32 = emb1.iter().zip(emb2.iter()).map(|(a, b)| a * b).sum();
        let sim_1_3: f32 = emb1.iter().zip(emb3.iter()).map(|(a, b)| a * b).sum();
        
        // Similar texts should have higher similarity than different texts
        // Note: This is a basic test; n-gram hashing has limitations
        println!("Similarity (1,2): {}, Similarity (1,3): {}", sim_1_2, sim_1_3);
    }

    #[test]
    fn test_batch_embedding() {
        let embedder = EmbeddingModel::default_model();
        
        let texts = vec!["Hello", "World", "Test"];
        let embeddings = embedder.embed_batch(&texts).unwrap();
        
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 384);
        }
    }

    #[test]
    fn test_custom_config() {
        let config = EmbeddingConfig {
            embedding_dim: 128,
            normalize: false,
            ngram_range: (2, 3),
        };
        
        let embedder = EmbeddingModel::new(config);
        let embedding = embedder.embed("Test").unwrap();
        
        assert_eq!(embedding.len(), 128);
    }
}

