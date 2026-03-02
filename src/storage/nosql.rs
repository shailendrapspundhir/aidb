use crate::storage::{Document, Storage};
use serde_json;

impl Storage {
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
        let metadata_batch = crate::storage::create_metadata_batch(&doc.id, &doc.text)?;
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

    /// Update NoSQL Document by ID (upsert JSON in Sled ; syncs metadata/vector)
    /// For edit capability in NoSQL layer.
    pub fn update_doc(&self, doc: Document, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize updated JSON
        let json_bytes = serde_json::to_vec(&doc)?;
        let key = format!("{}/{}", collection_id, doc.id);

        // Upsert in doc_tree (NoSQL)
        self.doc_tree.insert(key.as_bytes(), json_bytes)?;

        // Sync to Arrow/metadata + vector trees for SQL/index consistency
        let metadata_batch = crate::storage::create_metadata_batch(&doc.id, &doc.text)?;
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
}
