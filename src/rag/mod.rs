//! RAG (Retrieval-Augmented Generation) System Module
//!
//! This module provides comprehensive RAG capabilities for the AI database:
//! - Text tokenization and chunking
//! - Embedding generation using n-gram hashing
//! - Text and vector storage for AI-powered search
//!
//! The RAG system enables storing any text data and retrieving it using
//! semantic similarity search, making it ideal for LLM applications.
//!
//! # Example
//!
//! ```rust
//! use my_ai_db::rag::{RagPipeline, ChunkingConfig, EmbeddingConfig};
//!
//! // Create a RAG pipeline with default configuration
//! let pipeline = RagPipeline::simple().unwrap();
//!
//! // Generate an embedding for text
//! let embedding = pipeline.embed("Hello, world!").unwrap();
//! println!("Embedding dimension: {}", embedding.len());
//! ```

pub mod tokenizer;
pub mod embeddings;
pub mod pipeline;

pub use tokenizer::{TextTokenizer, TextChunk, ChunkingConfig};
pub use embeddings::{EmbeddingModel, EmbeddingConfig};
pub use pipeline::{RagPipeline, RagDocument, RagSearchResult};
