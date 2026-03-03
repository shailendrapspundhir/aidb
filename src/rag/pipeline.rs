//! RAG Pipeline Module
//!
//! Integrates tokenization, embedding generation, and storage for complete RAG workflows.
//! Provides high-level APIs for ingesting text and performing semantic search.

use serde::{Deserialize, Serialize};
use tracing::{info, debug, instrument};

use super::tokenizer::{TextTokenizer, TextChunk, ChunkingConfig};
use super::embeddings::{EmbeddingModel, EmbeddingConfig};
use crate::storage::Storage;
use crate::indexing::VectorIndex;

/// A RAG document with text and embedding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagDocument {
    /// Unique identifier for the document
    pub id: String,
    /// Original text content
    pub text: String,
    /// Vector embedding of the text
    pub embedding: Vec<f32>,
    /// Optional metadata
    pub metadata: serde_json::Value,
    /// Source identifier (e.g., file path, URL)
    pub source: Option<String>,
    /// Timestamp when document was created
    pub created_at: String,
}

/// Result from a RAG search query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagSearchResult {
    /// The matched document chunk
    pub chunk: TextChunk,
    /// Similarity score (lower is better for L2 distance)
    pub score: f32,
    /// The embedding of the result
    pub embedding: Vec<f32>,
}

/// Configuration for the RAG pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagPipelineConfig {
    /// Tokenizer model name (optional, for advanced chunking)
    pub tokenizer_model: Option<String>,
    /// Chunking configuration
    pub chunking: ChunkingConfig,
    /// Embedding configuration
    pub embedding: EmbeddingConfig,
}

impl Default for RagPipelineConfig {
    fn default() -> Self {
        Self {
            tokenizer_model: None, // Use simple chunking by default
            chunking: ChunkingConfig::default(),
            embedding: EmbeddingConfig::default(),
        }
    }
}

/// RAG Pipeline for text ingestion and semantic search
pub struct RagPipeline {
    tokenizer: Option<TextTokenizer>,
    embedder: EmbeddingModel,
    config: RagPipelineConfig,
}

impl RagPipeline {
    /// Create a new RAG pipeline with the given configuration
    #[instrument(skip(config))]
    pub fn new(config: RagPipelineConfig) -> Result<Self, Box<dyn std::error::Error>> {
        debug!("Initializing RAG pipeline");
        
        // Initialize tokenizer if model specified
        let tokenizer = if let Some(ref model) = config.tokenizer_model {
            TextTokenizer::new(model, config.chunking.clone()).ok()
        } else {
            None
        };
        
        // Initialize embedder
        let embedder = EmbeddingModel::new(config.embedding.clone());
        
        info!("RAG pipeline initialized successfully");
        Ok(Self {
            tokenizer,
            embedder,
            config,
        })
    }

    /// Create a RAG pipeline with default configuration (simple embedder)
    pub fn simple() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(RagPipelineConfig::default())
    }

    /// Ingest text into the RAG system
    /// 
    /// This method:
    /// 1. Chunks the text into smaller pieces
    /// 2. Generates embeddings for each chunk
    /// 3. Stores both text and embeddings in the database
    #[instrument(skip(self, storage, text), fields(text_len = text.len()))]
    pub async fn ingest_text(
        &self,
        storage: &Storage,
        text: &str,
        doc_id: &str,
        collection_id: &str,
        metadata: Option<serde_json::Value>,
        source: Option<String>,
    ) -> Result<Vec<TextChunk>, Box<dyn std::error::Error>> {
        debug!(doc_id = %doc_id, collection_id = %collection_id, "Ingesting text into RAG system");
        
        // Chunk the text
        let chunks = if let Some(ref tokenizer) = self.tokenizer {
            tokenizer.chunk_text(text, doc_id)?
        } else {
            // Fallback: simple character-based chunking
            self.simple_chunk(text, doc_id)?
        };
        
        info!(doc_id = %doc_id, chunk_count = chunks.len(), "Text chunked successfully");
        
        // Generate embeddings for each chunk
        let chunk_texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
        let embeddings = self.embedder.embed_batch(&chunk_texts)?;
        
        // Store each chunk with its embedding
        for (chunk, embedding) in chunks.iter().zip(embeddings.iter()) {
            let rag_doc = RagDocument {
                id: chunk.id.clone(),
                text: chunk.text.clone(),
                embedding: embedding.clone(),
                metadata: metadata.clone().unwrap_or(chunk.metadata.clone()),
                source: source.clone(),
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            
            self.store_document(storage, &rag_doc, collection_id)?;
        }
        
        info!(
            doc_id = %doc_id,
            collection_id = %collection_id,
            chunks_stored = chunks.len(),
            "Text ingestion completed"
        );
        
        Ok(chunks)
    }

    /// Simple character-based chunking fallback
    fn simple_chunk(&self, text: &str, doc_id: &str) -> Result<Vec<TextChunk>, Box<dyn std::error::Error>> {
        let max_chars = self.config.chunking.max_tokens * 4; // Rough estimate
        let overlap_chars = self.config.chunking.overlap_tokens * 4;
        
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut chunk_index = 0;
        
        while start < text.len() {
            let end = std::cmp::min(start + max_chars, text.len());
            let chunk_text = text[start..end].to_string();
            
            chunks.push(TextChunk {
                id: format!("{}-{}", doc_id, chunk_index),
                text: chunk_text,
                token_count: (end - start) / 4, // Rough estimate
                start_offset: start,
                end_offset: end,
                chunk_index,
                total_chunks: 0,
                metadata: serde_json::json!({}),
            });
            
            chunk_index += 1;
            start = if end >= text.len() {
                text.len()
            } else {
                end.saturating_sub(overlap_chars)
            };
        }
        
        // Update total_chunks
        let total = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total;
        }
        
        Ok(chunks)
    }

    /// Generate embedding for a single text
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        self.embedder.embed(text)
    }

    /// Generate embeddings for multiple texts
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        self.embedder.embed_batch(texts)
    }

    /// Store a RAG document in the database
    fn store_document(
        &self,
        storage: &Storage,
        doc: &RagDocument,
        collection_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::storage::Document;
        
        let storage_doc = Document {
            id: doc.id.clone(),
            text: doc.text.clone(),
            category: "rag".to_string(),
            vector: doc.embedding.clone(),
            metadata: serde_json::json!({
                "source": doc.source,
                "created_at": doc.created_at,
                "custom": doc.metadata,
            }),
        };
        
        storage.insert_doc(storage_doc, collection_id)?;
        Ok(())
    }

    /// Search for similar documents using a query
    #[instrument(skip(self, storage), fields(collection_id, top_k))]
    pub async fn search(
        &self,
        storage: &Storage,
        collection_id: &str,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<RagSearchResult>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, query_len = query.len(), top_k = top_k, "RAG search query");
        
        // Generate embedding for query
        let query_embedding = self.embed(query)?;
        
        // Get all vectors in collection
        let vectors = storage.get_vectors_in_collection(collection_id)?;
        
        if vectors.is_empty() {
            info!(collection_id = %collection_id, "No documents found in collection");
            return Ok(vec![]);
        }
        
        // Build vector index
        let index = VectorIndex::build_from_vectors(vectors);
        
        // Search for similar vectors
        let result_ids = index.search(&query_embedding, top_k);
        
        // Fetch full documents for results
        let mut results = Vec::new();
        for id in result_ids {
            if let Ok(doc) = storage.get_doc(collection_id, &id) {
                // Calculate similarity score (L2 distance)
                let score = self.calculate_distance(&query_embedding, &doc.vector);
                
                // Convert to TextChunk format
                let chunk = TextChunk {
                    id: doc.id.clone(),
                    text: doc.text.clone(),
                    token_count: 0, // We don't have this info stored
                    start_offset: 0,
                    end_offset: doc.text.len(),
                    chunk_index: 0,
                    total_chunks: 1,
                    metadata: doc.metadata,
                };
                
                results.push(RagSearchResult {
                    chunk,
                    score,
                    embedding: doc.vector,
                });
            }
        }
        
        info!(
            collection_id = %collection_id,
            results_count = results.len(),
            "RAG search completed"
        );
        
        Ok(results)
    }

    /// Calculate L2 distance between two vectors
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Get the embedding dimension
    pub fn embedding_dim(&self) -> usize {
        self.embedder.embedding_dim()
    }

    /// Get the configuration
    pub fn config(&self) -> &RagPipelineConfig {
        &self.config
    }

    /// Delete a document from the RAG system
    #[instrument(skip(self, storage), fields(doc_id, collection_id))]
    pub async fn delete_document(
        &self,
        storage: &Storage,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // First, find all chunks belonging to this document
        let docs = storage.get_docs_in_collection(collection_id)?;
        let chunks_to_delete: Vec<String> = docs
            .iter()
            .filter(|d| d.id.starts_with(&format!("{}-", doc_id)) || d.id == doc_id)
            .map(|d| d.id.clone())
            .collect();
        
        // Delete each chunk
        for chunk_id in chunks_to_delete {
            storage.delete_doc(collection_id, &chunk_id)?;
        }
        
        info!(
            collection_id = %collection_id,
            doc_id = %doc_id,
            "Document deleted from RAG system"
        );
        
        Ok(())
    }

    /// Update a document in the RAG system
    #[instrument(skip(self, storage, text), fields(doc_id, collection_id))]
    pub async fn update_document(
        &self,
        storage: &Storage,
        text: &str,
        doc_id: &str,
        collection_id: &str,
        metadata: Option<serde_json::Value>,
        source: Option<String>,
    ) -> Result<Vec<TextChunk>, Box<dyn std::error::Error>> {
        // Delete existing document chunks
        self.delete_document(storage, collection_id, doc_id).await?;
        
        // Re-ingest with new content
        self.ingest_text(storage, text, doc_id, collection_id, metadata, source).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rag_pipeline_config_default() {
        let config = RagPipelineConfig::default();
        assert!(config.tokenizer_model.is_none());
        assert_eq!(config.embedding.embedding_dim, 384);
    }

    #[test]
    fn test_simple_pipeline_creation() {
        let pipeline = RagPipeline::simple();
        assert!(pipeline.is_ok());
    }
}

