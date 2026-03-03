//! Text Tokenizer Module for RAG System
//!
//! Provides text tokenization and chunking capabilities using HuggingFace tokenizers.
//! Supports various chunking strategies for optimal RAG performance.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokenizers::{Tokenizer, Encoding};
use tracing::{info, debug, warn, error, instrument};

/// Configuration for text chunking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    /// Maximum number of tokens per chunk
    pub max_tokens: usize,
    /// Number of overlapping tokens between chunks
    pub overlap_tokens: usize,
    /// Whether to split on sentence boundaries
    pub sentence_boundary: bool,
    /// Minimum chunk size in tokens
    pub min_tokens: usize,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            max_tokens: 512,
            overlap_tokens: 50,
            sentence_boundary: true,
            min_tokens: 50,
        }
    }
}

/// Represents a chunk of text with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextChunk {
    /// Unique identifier for the chunk
    pub id: String,
    /// The text content of the chunk
    pub text: String,
    /// Token count for this chunk
    pub token_count: usize,
    /// Start position in original text (character offset)
    pub start_offset: usize,
    /// End position in original text (character offset)
    pub end_offset: usize,
    /// Index of this chunk in the sequence
    pub chunk_index: usize,
    /// Total number of chunks
    pub total_chunks: usize,
    /// Optional metadata
    pub metadata: serde_json::Value,
}

/// Text tokenizer using HuggingFace tokenizers library
pub struct TextTokenizer {
    tokenizer: Tokenizer,
    config: ChunkingConfig,
}

impl TextTokenizer {
    /// Create a new tokenizer from a local tokenizer.json file
    /// 
    /// # Arguments
    /// * `model_name` - Path to a tokenizer.json file
    /// * `config` - Chunking configuration
    #[instrument(skip(config), fields(model_name))]
    pub fn new(model_name: &str, config: ChunkingConfig) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(model_name = %model_name, "Loading tokenizer from file");

        if !std::path::Path::new(model_name).exists() {
            warn!(model_name = %model_name, "Tokenizer file not found");
            return Err(format!("Tokenizer file not found: {}", model_name).into());
        }

        let tokenizer = Tokenizer::from_file(model_name)
            .map_err(|e| format!("Failed to load tokenizer from file '{}': {}", model_name, e))?;
        
        info!(model_name = %model_name, max_tokens = config.max_tokens, "Tokenizer loaded successfully");
        Ok(Self { tokenizer, config })
    }

    /// Create a tokenizer from a local file
    #[instrument(skip(config, path))]
    pub fn from_file(path: &str, config: ChunkingConfig) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(path = %path, "Loading tokenizer from file");
        
        let tokenizer = Tokenizer::from_file(path)
            .map_err(|e| format!("Failed to load tokenizer from file '{}': {}", path, e))?;
        
        info!(path = %path, "Tokenizer loaded from file successfully");
        Ok(Self { tokenizer, config })
    }

    /// Create a default tokenizer from a local tokenizer.json file
    pub fn default_tokenizer() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new("tokenizer.json", ChunkingConfig::default())
    }

    /// Get the chunking configuration
    pub fn config(&self) -> &ChunkingConfig {
        &self.config
    }

    /// Tokenize text and return token count
    #[instrument(skip(self, text))]
    pub fn tokenize(&self, text: &str) -> Result<Encoding, Box<dyn std::error::Error>> {
        let encoding = self.tokenizer
            .encode(text, true)
            .map_err(|e| format!("Tokenization error: {}", e))?;
        Ok(encoding)
    }

    /// Get the number of tokens in a text
    #[instrument(skip(self, text))]
    pub fn count_tokens(&self, text: &str) -> Result<usize, Box<dyn std::error::Error>> {
        let encoding = self.tokenize(text)?;
        Ok(encoding.len())
    }

    /// Decode tokens back to text
    #[instrument(skip(self, tokens))]
    pub fn decode(&self, tokens: &[u32]) -> Result<String, Box<dyn std::error::Error>> {
        let text = self.tokenizer
            .decode(tokens, true)
            .map_err(|e| format!("Decoding error: {}", e))?;
        Ok(text)
    }

    /// Split text into chunks based on configuration
    /// 
    /// This method implements a sliding window approach with optional
    /// sentence boundary detection for better chunk quality.
    #[instrument(skip(self, text), fields(text_len = text.len()))]
    pub fn chunk_text(&self, text: &str, doc_id: &str) -> Result<Vec<TextChunk>, Box<dyn std::error::Error>> {
        debug!(doc_id = %doc_id, text_len = text.len(), "Starting text chunking");
        
        // First, encode the entire text
        let encoding = self.tokenize(text)?;
        let total_tokens = encoding.len();
        
        if total_tokens == 0 {
            warn!(doc_id = %doc_id, "Empty text provided for chunking");
            return Ok(vec![]);
        }

        // If text is shorter than max_tokens, return as single chunk
        if total_tokens <= self.config.max_tokens {
            debug!(doc_id = %doc_id, tokens = total_tokens, "Text fits in single chunk");
            return Ok(vec![TextChunk {
                id: format!("{}-0", doc_id),
                text: text.to_string(),
                token_count: total_tokens,
                start_offset: 0,
                end_offset: text.len(),
                chunk_index: 0,
                total_chunks: 1,
                metadata: serde_json::json!({}),
            }]);
        }

        // Get token offsets for character-level mapping
        let tokens = encoding.get_tokens();
        let offsets = encoding.get_offsets();
        
        // Calculate chunk boundaries
        let mut chunks = Vec::new();
        let mut chunk_index = 0;
        let mut start_token = 0;

        while start_token < total_tokens {
            // Calculate end token for this chunk
            let end_token = std::cmp::min(start_token + self.config.max_tokens, total_tokens);
            
            // Get character offsets for this chunk
            let start_char = offsets[start_token].0 as usize;
            let end_char = if end_token < total_tokens {
                offsets[end_token - 1].1 as usize
            } else {
                text.len()
            };

            // Extract chunk text
            let chunk_text = text[start_char..end_char].to_string();
            
            // Apply sentence boundary adjustment if enabled
            let (final_text, final_end_char) = if self.config.sentence_boundary && end_token < total_tokens {
                Self::adjust_to_sentence_boundary(&chunk_text, start_char, text)
            } else {
                (chunk_text, end_char)
            };

            // Only add chunk if it meets minimum size
            let chunk_tokens = self.count_tokens(&final_text)?;
            if chunk_tokens >= self.config.min_tokens || start_token + self.config.max_tokens >= total_tokens {
                chunks.push(TextChunk {
                    id: format!("{}-{}", doc_id, chunk_index),
                    text: final_text,
                    token_count: chunk_tokens,
                    start_offset: start_char,
                    end_offset: final_end_char,
                    chunk_index,
                    total_chunks: 0, // Will be updated later
                    metadata: serde_json::json!({}),
                });
                chunk_index += 1;
            }

            // Move to next chunk with overlap
            start_token = if end_token >= total_tokens {
                total_tokens
            } else {
                end_token.saturating_sub(self.config.overlap_tokens)
            };
        }

        // Update total_chunks for all chunks
        let total_chunks = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total_chunks;
        }

        info!(doc_id = %doc_id, total_chunks = total_chunks, total_tokens = total_tokens, "Text chunking completed");
        Ok(chunks)
    }

    /// Adjust chunk boundary to end at a sentence boundary
    fn adjust_to_sentence_boundary(chunk_text: &str, start_char: usize, original_text: &str) -> (String, usize) {
        // Look for sentence-ending punctuation
        let sentence_enders = ['.', '!', '?', '\n'];
        
        // Find the last sentence ender in the chunk
        if let Some(pos) = chunk_text.rfind(|c| sentence_enders.contains(&c)) {
            let adjusted_text = chunk_text[..=pos].to_string();
            let adjusted_end = start_char + pos + 1;
            (adjusted_text, adjusted_end)
        } else {
            (chunk_text.to_string(), start_char + chunk_text.len())
        }
    }

    /// Chunk text with custom metadata for each chunk
    #[instrument(skip(self, text, metadata))]
    pub fn chunk_text_with_metadata(
        &self,
        text: &str,
        doc_id: &str,
        metadata: serde_json::Value,
    ) -> Result<Vec<TextChunk>, Box<dyn std::error::Error>> {
        let mut chunks = self.chunk_text(text, doc_id)?;
        
        // Add metadata to each chunk
        for chunk in &mut chunks {
            chunk.metadata = metadata.clone();
        }
        
        Ok(chunks)
    }

    /// Get the vocabulary size of the tokenizer
    pub fn vocab_size(&self) -> usize {
        self.tokenizer.get_vocab_size(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer_creation() {
        let config = ChunkingConfig {
            max_tokens: 100,
            overlap_tokens: 10,
            sentence_boundary: true,
            min_tokens: 10,
        };
        
        // This test expects a local tokenizer.json file
        if let Ok(tokenizer) = TextTokenizer::new("tokenizer.json", config) {
            assert!(tokenizer.vocab_size() > 0);
        }
    }

    #[test]
    fn test_chunking_config_default() {
        let config = ChunkingConfig::default();
        assert_eq!(config.max_tokens, 512);
        assert_eq!(config.overlap_tokens, 50);
        assert!(config.sentence_boundary);
        assert_eq!(config.min_tokens, 50);
    }
}
