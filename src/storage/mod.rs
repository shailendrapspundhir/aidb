use serde::{Deserialize, Serialize};
use serde_json;
use sled::Db;
use std::sync::{Arc, Mutex};
use tracing::{info, debug, warn, error, instrument};

use crate::cache::DocCache;

pub mod nosql;
pub mod sql;
pub mod vector;

pub use vector::create_metadata_batch;
pub use nosql::RagStorageDocument;

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
    pub(crate) rag_tree: sled::Tree,  // For RAG documents and chunks
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
    /// - RAG tree for RAG documents and chunks
    #[instrument(skip(path), fields(path))]
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(path = %path, "Opening storage");
        
        let db = sled::open(path)?;
        let metadata_tree = db.open_tree("metadata")?;
        let vector_tree = db.open_tree("vectors")?;
        let doc_tree = db.open_tree("docs")?;  // NoSQL JSON storage
        let user_tree = db.open_tree("users")?;
        let tenant_tree = db.open_tree("tenants")?;
        let env_tree = db.open_tree("environments")?;
        let collection_tree = db.open_tree("collections")?;
        let rag_tree = db.open_tree("rag")?;  // RAG documents and chunks
        let capacity_mb = read_cache_capacity_mb();
        let capacity_bytes = capacity_mb.saturating_mul(1024).saturating_mul(1024);
        
        info!(
            path = %path,
            cache_capacity_mb = capacity_mb,
            "Storage opened successfully"
        );
        
        Ok(Self {
            db,
            metadata_tree,
            vector_tree,
            doc_tree,
            user_tree,
            tenant_tree,
            env_tree,
            collection_tree,
            rag_tree,
            doc_cache: Arc::new(Mutex::new(DocCache::new(capacity_bytes))),
        })
    }
}

use async_trait::async_trait;
use crate::events::{PubSubManager, CdcEvent, EventType};
use chrono::Utc;

/// StorageEngine trait for abstracting storage operations
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn insert(&self, collection: &str, id: &str, document: serde_json::Value) -> Result<(), String>;
    async fn get(&self, collection: &str, id: &str) -> Result<Option<serde_json::Value>, String>;
    async fn update(&self, collection: &str, id: &str, document: serde_json::Value) -> Result<(), String>;
    async fn delete(&self, collection: &str, id: &str) -> Result<(), String>;
    async fn list_collections(&self) -> Result<Vec<String>, String>;
}

/// CDC Wrapper that wraps any StorageEngine and emits events on mutations
pub struct CdcWrapper<T: StorageEngine> {
    engine: Arc<T>,
    pubsub: Arc<PubSubManager>,
}

impl<T: StorageEngine> CdcWrapper<T> {
    pub fn new(engine: Arc<T>, pubsub: Arc<PubSubManager>) -> Self {
        Self { engine, pubsub }
    }
}

#[async_trait]
impl<T: StorageEngine> StorageEngine for CdcWrapper<T> {
    async fn insert(&self, collection: &str, id: &str, document: serde_json::Value) -> Result<(), String> {
        let doc_clone = document.clone();
        self.engine.insert(collection, id, document).await?;
        self.pubsub.publish(CdcEvent {
            event_type: EventType::Insert,
            collection: collection.to_string(),
            id: id.to_string(),
            data: Some(doc_clone),
            timestamp: Utc::now().timestamp(),
        });
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<serde_json::Value>, String> {
        self.engine.get(collection, id).await
    }

    async fn update(&self, collection: &str, id: &str, document: serde_json::Value) -> Result<(), String> {
        let doc_clone = document.clone();
        self.engine.update(collection, id, document).await?;
        self.pubsub.publish(CdcEvent {
            event_type: EventType::Update,
            collection: collection.to_string(),
            id: id.to_string(),
            data: Some(doc_clone),
            timestamp: Utc::now().timestamp(),
        });
        Ok(())
    }

    async fn delete(&self, collection: &str, id: &str) -> Result<(), String> {
        self.engine.delete(collection, id).await?;
        self.pubsub.publish(CdcEvent {
            event_type: EventType::Delete,
            collection: collection.to_string(),
            id: id.to_string(),
            data: None,
            timestamp: Utc::now().timestamp(),
        });
        Ok(())
    }

    async fn list_collections(&self) -> Result<Vec<String>, String> {
        self.engine.list_collections().await
    }
}
