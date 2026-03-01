use arrow::array::{ArrayRef, StringArray};  // Float32Array unused after placeholder schema
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json;
use sled::Db;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use crate::models::{User, Tenant, Environment, Collection};

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
    metadata_tree: sled::Tree,
    vector_tree: sled::Tree,
    doc_tree: sled::Tree,  // For NoSQL/JSON docs
    user_tree: sled::Tree,
    tenant_tree: sled::Tree,
    env_tree: sled::Tree,
    collection_tree: sled::Tree,
    doc_cache: Arc<Mutex<DocCache>>, // In-memory cache for docs
}

#[derive(Clone, Debug)]
struct CacheEntry {
    doc: Document,
    size_bytes: usize,
}

#[derive(Debug)]
struct DocCache {
    capacity_bytes: usize,
    size_bytes: usize,
    entries: HashMap<String, CacheEntry>,
    lru_order: VecDeque<String>,
}

impl DocCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            size_bytes: 0,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
        }
    }

    fn get(&mut self, id: &str) -> Option<Document> {
        if let Some(entry) = self.entries.get(id) {
            let doc_clone = entry.doc.clone();
            self.touch(id);
            return Some(doc_clone);
        }
        None
    }

    fn insert(&mut self, id: String, doc: Document) {
        let size_bytes = estimate_doc_size_bytes(&doc);
        if size_bytes > self.capacity_bytes {
            return;
        }
        if let Some(existing) = self.entries.remove(&id) {
            self.size_bytes = self.size_bytes.saturating_sub(existing.size_bytes);
            self.lru_order.retain(|key| key != &id);
        }
        while self.size_bytes + size_bytes > self.capacity_bytes {
            if let Some(evict_id) = self.lru_order.pop_back() {
                if let Some(evicted) = self.entries.remove(&evict_id) {
                    self.size_bytes = self.size_bytes.saturating_sub(evicted.size_bytes);
                }
            } else {
                break;
            }
        }
        self.size_bytes += size_bytes;
        self.lru_order.push_front(id.clone());
        self.entries.insert(id, CacheEntry { doc, size_bytes });
    }

    fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.size_bytes = self.size_bytes.saturating_sub(entry.size_bytes);
            self.lru_order.retain(|key| key != id);
        }
    }

    fn touch(&mut self, id: &str) {
        self.lru_order.retain(|key| key != id);
        self.lru_order.push_front(id.to_string());
    }
}

fn estimate_doc_size_bytes(doc: &Document) -> usize {
    doc.id.len()
        + doc.text.len()
        + doc.category.len()
        + doc.vector.len() * std::mem::size_of::<f32>()
        + doc.metadata.to_string().len()
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

    // User CRUD
    pub fn create_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        let key = user.username.as_bytes();
        if self.user_tree.contains_key(key)? {
            return Err("User already exists".into());
        }
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(key, value)?;
        Ok(())
    }

    pub fn get_user(&self, username: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
        if let Some(value) = self.user_tree.get(username.as_bytes())? {
            let user: User = serde_json::from_slice(&value)?;
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    pub fn update_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(user.username.as_bytes(), value)?;
        Ok(())
    }

    // Tenant CRUD
    pub fn create_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_tenant(&self, id: &str) -> Result<Option<Tenant>, Box<dyn std::error::Error>> {
        if let Some(value) = self.tenant_tree.get(id.as_bytes())? {
            let tenant: Tenant = serde_json::from_slice(&value)?;
            Ok(Some(tenant))
        } else {
            Ok(None)
        }
    }

    pub fn update_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        Ok(())
    }

    // Environment CRUD
    pub fn create_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_environment(&self, id: &str) -> Result<Option<Environment>, Box<dyn std::error::Error>> {
        if let Some(value) = self.env_tree.get(id.as_bytes())? {
            let env: Environment = serde_json::from_slice(&value)?;
            Ok(Some(env))
        } else {
            Ok(None)
        }
    }

    pub fn update_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        Ok(())
    }

    // Collection CRUD
    pub fn create_collection(&self, col: Collection) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&col)?;
        self.collection_tree.insert(col.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_collection(&self, id: &str) -> Result<Option<Collection>, Box<dyn std::error::Error>> {
        if let Some(value) = self.collection_tree.get(id.as_bytes())? {
            let col: Collection = serde_json::from_slice(&value)?;
            Ok(Some(col))
        } else {
            Ok(None)
        }
    }

    /// Insert an Arrow RecordBatch (metadata) and a vector for a given ID
    pub fn insert(
        &self,
        id: &str,
        metadata_batch: RecordBatch,
        vector: Vec<f32>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize metadata RecordBatch to IPC bytes
        let mut metadata_buf = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut metadata_buf, metadata_batch.schema().as_ref())?;
            writer.write(&metadata_batch)?;
            writer.finish()?;
        }

        // Serialize vector to bytes (little endian f32)
        let vector_bytes: Vec<u8> = vector
            .iter()
            .flat_map(|&f| f.to_le_bytes().to_vec())
            .collect();

        // Store with id as key in respective trees
        self.metadata_tree.insert(id.as_bytes(), metadata_buf)?;
        self.vector_tree.insert(id.as_bytes(), vector_bytes)?;

        Ok(())
    }

    /// Retrieve Arrow RecordBatch (metadata) and vector by ID
    /// (Legacy vector-specific getter; see get_doc for NoSQL)
    pub fn get(
        &self,
        id: &str,
    ) -> Result<(RecordBatch, Vec<f32>), Box<dyn std::error::Error>> {
        // Get metadata
        if let Some(metadata_bytes) = self.metadata_tree.get(id.as_bytes())? {
            let cursor = Cursor::new(metadata_bytes);
            let mut reader = FileReader::try_new(cursor, None)?;
            let batch = reader
                .next()
                .ok_or("No batch found in IPC data")??
                .clone();
            // Get vector
            if let Some(vector_bytes) = self.vector_tree.get(id.as_bytes())? {
                let vec_bytes = vector_bytes.to_vec();
                let mut vector = Vec::new();
                for chunk in vec_bytes.chunks_exact(4) {
                    let f = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    vector.push(f);
                }
                Ok((batch, vector))
            } else {
                Err("Vector not found".into())
            }
        } else {
            Err("Metadata not found".into())
        }
    }

    // --- Multi-Model Extensions for NoSQL (JSON) & SQL Hybrid ---

    /// Insert a NoSQL Document (JSON via Serde) into unified Sled storage
    /// This provides schema-flexible document storage. Automatically syncs
    /// vector/metadata for indexing. Core to unified KV layer.
    pub fn insert_doc(&self, doc: Document, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize to JSON bytes for NoSQL storage in Sled
        let json_bytes = serde_json::to_vec(&doc)?;
        let key = format!("{}/{}", collection_id, doc.id);

        // Store raw JSON doc (NoSQL)
        self.doc_tree.insert(key.as_bytes(), json_bytes)?;

        // Sync to existing vector/Arrow for compatibility (hybrid link)
        let metadata_batch = create_metadata_batch(&doc.id, &doc.text)?;
        self.insert(&key, metadata_batch, doc.vector.clone())?;  // Reuses vector storage

        // Update cache
        if let Ok(mut cache) = self.doc_cache.lock() {
            cache.insert(key, doc);
        }

        Ok(())
    }

    /// Retrieve NoSQL Document by ID (deserializes JSON from Sled)
    /// Enables dynamic/unstructured access.
    pub fn get_doc(&self, collection_id: &str, id: &str) -> Result<Document, Box<dyn std::error::Error>> {
        let key = format!("{}/{}", collection_id, id);
        let (doc, _) = self.get_doc_with_cache_status(&key)?;
        Ok(doc)
    }

    /// Retrieve NoSQL Document by ID, returning if it was served from cache.
    pub fn get_doc_with_cache_status(
        &self,
        key: &str,
    ) -> Result<(Document, bool), Box<dyn std::error::Error>> {
        if let Ok(mut cache) = self.doc_cache.lock() {
            if let Some(doc) = cache.get(key) {
                return Ok((doc, true));
            }
        }

        if let Some(doc_bytes) = self.doc_tree.get(key.as_bytes())? {
            let doc: Document = serde_json::from_slice(&doc_bytes)?;
            if let Ok(mut cache) = self.doc_cache.lock() {
                cache.insert(key.to_string(), doc.clone());
            }
            Ok((doc, false))
        } else {
            Err("Document not found".into())
        }
    }

    /// Project NoSQL docs from Sled into Arrow RecordBatch
    /// This is the hybrid link: Enables SQL queries via DataFusion on
    /// structured view of JSON data (high-perf vectorized scans).
    /// Supports push-down filters for category, text, etc.
    /// Fixed schema: basic columns to ensure DataFusion table register/query success
    /// (vector stringified for hybrid; full List for prod).
    pub fn project_collection_to_arrow(&self, collection_id: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut ids = vec![];
        let mut texts = vec![];
        let mut categories = vec![];
        let mut vector_strs = vec![];  // Stringify vectors for SQL compat

        let prefix = format!("{}/", collection_id);

        // Scan NoSQL docs from Sled
        for item in self.doc_tree.scan_prefix(prefix.as_bytes()) {
            let (_, value) = item?;
            let doc: Document = serde_json::from_slice(&value)?;
            ids.push(doc.id);
            texts.push(doc.text);
            categories.push(doc.category);
            // Stringify vector for placeholder (enables SQL , hybrid join)
            vector_strs.push(serde_json::to_string(&doc.vector).unwrap_or_default());
        }

        if ids.is_empty() {
            // Empty batch fallback for SQL register (prevents query fail on no data)
            ids.push("".to_string());
            texts.push("".to_string());
            categories.push("".to_string());
            vector_strs.push("[]".to_string());
        }

        // Build simple Arrow schema for SQL (avoids type errors , ensures response)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("vector", DataType::Utf8, false),  // Stringified for compat
        ]));

        // Convert to Arrow arrays (vectorized ; ids moved handled by len capture)
        let _num_rows = ids.len();  // Unused after simplification; prefix for warning
        let id_array = StringArray::from(ids);
        let text_array = StringArray::from(texts);
        let cat_array = StringArray::from(categories);
        let vec_str_array = StringArray::from(vector_strs);

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(text_array) as ArrayRef,
                Arc::new(cat_array) as ArrayRef,
                Arc::new(vec_str_array) as ArrayRef,
            ],
        )?)
    }

    /// Get all NoSQL docs (for hybrid planner/indexing)
    pub fn get_docs_in_collection(&self, collection_id: &str) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        let mut docs = vec![];
        let prefix = format!("{}/", collection_id);
        for item in self.doc_tree.scan_prefix(prefix.as_bytes()) {
            let (_, v) = item?;
            let doc: Document = serde_json::from_slice(&v)?;
            docs.push(doc);
        }
        Ok(docs)
    }

    // --- Additional NoSQL CRUD for full multi-model support (JSON in Sled) ---
    // Enables edit/delete alongside insert/query ; syncs to Arrow/vectors for SQL/vector consistency

    /// Update NoSQL Document by ID (upsert JSON in Sled ; syncs metadata/vector)
    /// For edit capability in NoSQL layer.
    pub fn update_doc(&self, doc: Document, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize updated JSON
        let json_bytes = serde_json::to_vec(&doc)?;
        let key = format!("{}/{}", collection_id, doc.id);

        // Upsert in doc_tree (NoSQL)
        self.doc_tree.insert(key.as_bytes(), json_bytes)?;

        // Sync to Arrow/metadata + vector trees for SQL/index consistency
        let metadata_batch = create_metadata_batch(&doc.id, &doc.text)?;
        self.insert(&key, metadata_batch, doc.vector.clone())?;

        if let Ok(mut cache) = self.doc_cache.lock() {
            cache.insert(key, doc);
        }

        Ok(())
    }

    /// Delete by ID from NoSQL (JSON) + synced trees (for unified cleanup)
    pub fn delete_doc(&self, collection_id: &str, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("{}/{}", collection_id, id);
        self.doc_tree.remove(key.as_bytes())?;
        self.metadata_tree.remove(key.as_bytes())?;
        self.vector_tree.remove(key.as_bytes())?;
        if let Ok(mut cache) = self.doc_cache.lock() {
            cache.remove(&key);
        }
        // Note: For full SQL sync , re-project Arrow table post-delete in prod
        Ok(())
    }

    /// Delete an entire collection and its documents
    pub fn delete_collection(&self, env_id: &str, col_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Remove all docs in collection from doc_tree, metadata_tree, vector_tree
        let prefix = format!("{}/", col_id);
        for item in self.doc_tree.scan_prefix(prefix.as_bytes()) {
            let (k, _) = item?;
            self.doc_tree.remove(&k)?;
            self.metadata_tree.remove(&k)?;
            self.vector_tree.remove(&k)?;

            // Cleanup cache if needed
            if let Ok(k_str) = String::from_utf8(k.to_vec()) {
                if let Ok(mut cache) = self.doc_cache.lock() {
                    cache.remove(&k_str);
                }
            }
        }

        // 2. Remove collection metadata
        self.collection_tree.remove(col_id.as_bytes())?;

        // 3. Update environment to remove collection ID
        if let Some(mut env) = self.get_environment(env_id)? {
            env.collections.retain(|id| id != col_id);
            self.update_environment(env)?;
        }

        Ok(())
    }

    /// Get all vectors for indexing purposes (returns id and vector)
    pub fn get_vectors_in_collection(&self, collection_id: &str) -> Result<Vec<(String, Vec<f32>)>, Box<dyn std::error::Error>> {
        let mut vectors = Vec::new();
        let prefix = format!("{}/", collection_id);
        // Vectors are in vector_tree. The key is same as doc key: col_id/doc_id
        for item in self.vector_tree.scan_prefix(prefix.as_bytes()) {
            let (k, v) = item?;
            let key_str = String::from_utf8(k.to_vec())?;
            // Extract doc_id from key "col_id/doc_id"
            let parts: Vec<&str> = key_str.split('/').collect();
            let id = if parts.len() > 1 { parts[1].to_string() } else { key_str }; // fallback

            let vec_bytes = v.to_vec();
            let mut vector = Vec::new();
            for chunk in vec_bytes.chunks_exact(4) {
                let f = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                vector.push(f);
            }
            vectors.push((id, vector));
        }
        Ok(vectors)
    }
}

/// Helper to create a sample metadata RecordBatch for an item
pub fn create_metadata_batch(id: &str, text: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
    ]));

    let id_array = StringArray::from(vec![id]);
    let text_array = StringArray::from(vec![text]);

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(text_array) as ArrayRef,
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use serde_json;  // For json! in multi-model test
    use std::fs;

    #[test]
    fn test_insert_and_retrieve_arrow_and_vector() {
        // Use temp dir for isolated test DB
        let temp_dir = std::env::temp_dir().join("aidb_test_storage");
        let _ = fs::remove_dir_all(&temp_dir); // Clean up previous test data

        let storage = Storage::open(temp_dir.to_str().unwrap()).expect("Failed to open storage");

        // Create and insert Arrow metadata record + vector
        let batch = create_metadata_batch("test_doc", "This is sample metadata for AI DB vector").unwrap();
        let vector: Vec<f32> = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        storage.insert("test_doc", batch.clone(), vector.clone()).expect("Insert failed");

        // Retrieve and verify
        let (retrieved_batch, retrieved_vector) = storage.get("test_doc").expect("Retrieve failed");
        assert_eq!(retrieved_vector, vector);

        // Verify Arrow record metadata
        let text_col = retrieved_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Text column not found");
        assert_eq!(text_col.value(0), "This is sample metadata for AI DB vector");

        // Clean up
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_multi_model_nosql_json_and_sql_projection() {
        // Temp DB for multi-model test (NoSQL JSON + SQL Arrow)
        let temp_dir = std::env::temp_dir().join("aidb_test_multimodel");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::open(temp_dir.to_str().unwrap()).expect("Failed to open storage");

        // Create NoSQL Document (JSON/Serde + vector for hybrid)
        let metadata_json = serde_json::json!({"tags": ["test", "ai"], "score": 42});
        let doc = Document {
            id: "multi_doc".to_string(),
            text: "Multi-model doc: SQL/NoSQL/Vector".to_string(),
            category: "AI".to_string(),
            vector: vec![0.5, 0.5, 0.5, 0.5],
            metadata: metadata_json,
        };

        // Insert to unified Sled (NoSQL JSON)
        storage.insert_doc(doc.clone()).expect("NoSQL insert failed");

        // Verify NoSQL retrieval (JSON deserial)
        let retrieved_doc = storage.get_doc("multi_doc").expect("NoSQL get failed");
        assert_eq!(retrieved_doc.id, "multi_doc");
        assert_eq!(retrieved_doc.category, "AI");

        // Verify hybrid link: project to Arrow for SQL/DataFusion
        let arrow_batch = storage.project_to_arrow().expect("SQL projection failed");
        assert!(arrow_batch.num_rows() > 0);
        // Check SQL fields (push-down capable)
        let cat_col = arrow_batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(cat_col.iter().any(|v| v == Some("AI")));

        // Clean up
        let _ = fs::remove_dir_all(temp_dir);
    }
}
