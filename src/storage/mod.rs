use serde::{Deserialize, Serialize};
use serde_json;
use sled::Db;
use std::sync::{Arc, Mutex};

use crate::cache::DocCache;

pub mod nosql;
pub mod sql;
pub mod vector;

pub use vector::create_metadata_batch;

/// Document struct for NoSQL/JSON support
/// Enables schema-flexible storage in Sled (Serde-serialized).
/// Fields projected to Arrow for SQL via DataFusion.
/// Unified with vectors for hybrid queries.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Document {
    pub id: String,
    pub text: String,      // Unstructured text
    pub category: String,  // For SQL filtering (e.g., 'AI')
    pub vector: Vec<f32>,  // Embedded vector for ANN
    pub metadata: serde_json::Value,  // Flexible JSON for extra NoSQL fields
}

#[allow(dead_code)]  // db kept for future ops like flush/close on Sled
#[derive(Clone)]  // Clone for sharing across gRPC/REST servers (Sled internals cheap to clone)
pub struct Storage {
    db: Db,
    // Trees for unified multi-model storage:
    // - metadata/vectors: existing vector + Arrow
    // - docs: NoSQL JSON documents (Serde-serialized for schema-flexible storage)
    pub(crate) metadata_tree: sled::Tree,
    pub(crate) vector_tree: sled::Tree,
    pub(crate) doc_tree: sled::Tree,  // For NoSQL/JSON docs
    pub(crate) user_tree: sled::Tree,
    pub(crate) tenant_tree: sled::Tree,
    pub(crate) env_tree: sled::Tree,
    pub(crate) collection_tree: sled::Tree,
    pub(crate) doc_cache: Arc<Mutex<DocCache>>, // In-memory cache for docs
}

fn read_cache_capacity_mb() -> usize {
    let raw = std::env::var("AIDB_CACHE_MB").unwrap_or_else(|_| "64".to_string());
    raw.trim().parse::<usize>().unwrap_or(64)
}

impl Storage {
    /// Open or create the Sled database at the given path
    /// Initializes unified trees for multi-model support:
    /// - Vectors/metadata for embeddings
    /// - Docs for NoSQL JSON (schema-flexible documents)
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db = sled::open(path)?;
        let metadata_tree = db.open_tree("metadata")?;
        let vector_tree = db.open_tree("vectors")?;
        let doc_tree = db.open_tree("docs")?;  // NoSQL JSON storage
        let user_tree = db.open_tree("users")?;
        let tenant_tree = db.open_tree("tenants")?;
        let env_tree = db.open_tree("environments")?;
        let collection_tree = db.open_tree("collections")?;
        let capacity_mb = read_cache_capacity_mb();
        let capacity_bytes = capacity_mb.saturating_mul(1024).saturating_mul(1024);
        Ok(Self {
            db,
            metadata_tree,
            vector_tree,
            doc_tree,
            user_tree,
            tenant_tree,
            env_tree,
            collection_tree,
            doc_cache: Arc::new(Mutex::new(DocCache::new(capacity_bytes))),
        })
    }
}
