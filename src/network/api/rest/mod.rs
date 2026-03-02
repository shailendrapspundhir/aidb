//! REST API layer for aiDB using Axum (exposed on port 11111)
//!
//! Provides HTTP/JSON endpoints mirroring multi-model gRPC functionality:
//! - NoSQL inserts, SQL queries (DataFusion), hybrid search (SQL + vector + NoSQL).
//! - Enables easy curl access and web integration alongside gRPC.
//! - Shared state with Storage/QueryEngine for unified layer; Tokio-compatible.

pub mod handlers;
pub mod middleware;
pub mod models;
pub mod router;
pub mod state;

pub use router::create_router;
pub use state::AppState;