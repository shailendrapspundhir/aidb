//! aiDB: Hybrid Vector Database in Rust
//!
//! Modular stack for high-performance storage (Sled + Arrow), advanced indexing (instant-distance HNSW),
//! data processing (DataFusion), and networking (Tonic gRPC).
//!
//! This lib exposes the core storage and indexing engine.

pub mod storage;
pub mod indexing;
// Query module for SQL engine (DataFusion) over NoSQL/Arrow projection
// Enables multi-model: SQL on JSON/vectors with push-down
pub mod query;
// REST API module: Axum HTTP handlers on port 11111 (mirrors gRPC for multi-model)
pub mod rest;
