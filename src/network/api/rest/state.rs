use crate::storage::Storage;
use std::sync::Arc;

/// Shared app state for REST handlers (Arc-wrapped for concurrency)
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
}