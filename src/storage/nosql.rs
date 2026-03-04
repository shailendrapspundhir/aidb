use crate::storage::{Document, Storage};
use serde_json;
use tracing::{info, debug, warn, error, instrument};

impl Storage {
    /// Insert a NoSQL Document (JSON via Serde) into unified Sled storage
    /// This provides schema-flexible document storage. Automatically syncs
    /// vector/metadata for indexing. Core to unified KV layer.
    #[instrument(skip(self, doc), fields(id = %doc.id, collection_id))]
    pub fn insert_doc(&self, doc: Document, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(id = %doc.id, collection_id = %collection_id, "Inserting NoSQL document");
        
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
            cache.insert(key.clone(), doc.clone());
        }
        
        info!(id = %doc.id, collection_id = %collection_id, "NoSQL document inserted successfully");
        Ok(())
    }

    /// Insert multiple NoSQL Documents (batch) into unified Sled storage
    #[instrument(skip(self, docs), fields(count = docs.len(), collection_id))]
    pub fn insert_docs(&self, docs: Vec<Document>, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(count = docs.len(), collection_id = %collection_id, "Inserting batch of NoSQL documents");
        
        let mut doc_batch = sled::Batch::default();
        let mut metadata_batch_op = sled::Batch::default();
        let mut vector_batch = sled::Batch::default();

        for doc in &docs {
            let json_bytes = serde_json::to_vec(doc)?;
            let key = format!("{}/{}", collection_id, doc.id);
            doc_batch.insert(key.as_bytes(), json_bytes);

            // Sync to vector/Arrow
            let metadata_batch = crate::storage::create_metadata_batch(&doc.id, &doc.text)?;
            
            // Serialize metadata RecordBatch to IPC bytes (inline logic from Storage::insert)
            let mut metadata_buf = Vec::new();
            {
                use arrow::ipc::writer::FileWriter;
                let mut writer = FileWriter::try_new(&mut metadata_buf, metadata_batch.schema().as_ref())?;
                writer.write(&metadata_batch)?;
                writer.finish()?;
            }
            metadata_batch_op.insert(key.as_bytes(), metadata_buf);

            // Serialize vector to bytes
            let vector_bytes: Vec<u8> = doc.vector
                .iter()
                .flat_map(|&f| f.to_le_bytes().to_vec())
                .collect();
            vector_batch.insert(key.as_bytes(), vector_bytes);
        }

        // Apply batches
        self.doc_tree.apply_batch(doc_batch)?;
        self.metadata_tree.apply_batch(metadata_batch_op)?;
        self.vector_tree.apply_batch(vector_batch)?;

        let docs_len = docs.len();

        // Update cache
        if let Ok(mut cache) = self.doc_cache.lock() {
            for doc in docs {
                let key = format!("{}/{}", collection_id, doc.id);
                cache.insert(key, doc);
            }
        }
        
        info!(count = docs_len, collection_id = %collection_id, "Batch insertion successful");
        Ok(())
    }

    /// Retrieve NoSQL Document by ID (deserializes JSON from Sled)
    /// Enables dynamic/unstructured access.
    #[instrument(skip(self), fields(key))]
    pub fn get_doc(&self, collection_id: &str, id: &str) -> Result<Document, Box<dyn std::error::Error>> {
        let key = format!("{}/{}", collection_id, id);
        debug!(key = %key, "Retrieving document");
        let (doc, _) = self.get_doc_with_cache_status(&key)?;
        info!(key = %key, "Document retrieved successfully");
        Ok(doc)
    }

    /// Retrieve NoSQL Document by ID, returning if it was served from cache.
    #[instrument(skip(self), fields(key))]
    pub fn get_doc_with_cache_status(
        &self,
        key: &str,
    ) -> Result<(Document, bool), Box<dyn std::error::Error>> {
        // Check cache first
        if let Ok(mut cache) = self.doc_cache.lock() {
            if let Some(doc) = cache.get(key) {
                debug!(key = %key, "Document served from cache");
                return Ok((doc, true));
            }
        }

        // Fetch from storage
        if let Some(doc_bytes) = self.doc_tree.get(key.as_bytes())? {
            let doc: Document = serde_json::from_slice(&doc_bytes)?;
            if let Ok(mut cache) = self.doc_cache.lock() {
                cache.insert(key.to_string(), doc.clone());
            }
            debug!(key = %key, "Document retrieved from storage");
            Ok((doc, false))
        } else {
            warn!(key = %key, "Document not found");
            Err("Document not found".into())
        }
    }

    /// Get all NoSQL docs (for hybrid planner/indexing)
    #[instrument(skip(self))]
    pub fn get_docs_in_collection(&self, collection_id: &str) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Retrieving all documents in collection");
        let mut docs = vec![];
        let prefix = format!("{}/", collection_id);
        for item in self.doc_tree.scan_prefix(prefix.as_bytes()) {
            let (_, v) = item?;
            let doc: Document = serde_json::from_slice(&v)?;
            docs.push(doc);
        }
        info!(collection_id = %collection_id, count = docs.len(), "Documents retrieved");
        Ok(docs)
    }

    /// Full/partial text search across documents in a collection
    #[instrument(skip(self, query), fields(collection_id, partial_match, case_sensitive, include_metadata))]
    pub fn search_docs_text(
        &self,
        collection_id: &str,
        query: &str,
        partial_match: bool,
        case_sensitive: bool,
        include_metadata: bool,
    ) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, query = %query, "Text search request");
        let docs = self.get_docs_in_collection(collection_id)?;
        let query_norm = if case_sensitive {
            query.to_string()
        } else {
            query.to_lowercase()
        };

        let mut matches = Vec::new();
        for doc in docs {
            let mut haystack = doc.text.clone();
            if include_metadata {
                haystack.push(' ');
                haystack.push_str(&doc.category);
                haystack.push(' ');
                if let Ok(meta_str) = serde_json::to_string(&doc.metadata) {
                    haystack.push_str(&meta_str);
                }
            }

            if !case_sensitive {
                haystack = haystack.to_lowercase();
            }

            let is_match = if partial_match {
                haystack.contains(&query_norm)
            } else {
                haystack
                    .split(|c: char| !c.is_alphanumeric())
                    .any(|token| token == query_norm)
            };

            if is_match {
                matches.push(doc);
            }
        }

        info!(collection_id = %collection_id, match_count = matches.len(), "Text search completed");
        Ok(matches)
    }

    /// Update NoSQL Document by ID (upsert JSON in Sled ; syncs metadata/vector)
    /// For edit capability in NoSQL layer.
    #[instrument(skip(self, doc), fields(id = %doc.id, collection_id))]
    pub fn update_doc(&self, doc: Document, collection_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(id = %doc.id, collection_id = %collection_id, "Updating NoSQL document");
        
        // Serialize updated JSON
        let json_bytes = serde_json::to_vec(&doc)?;
        let key = format!("{}/{}", collection_id, doc.id);

        // Upsert in doc_tree (NoSQL)
        self.doc_tree.insert(key.as_bytes(), json_bytes)?;

        // Sync to Arrow/metadata + vector trees for SQL/index consistency
        let metadata_batch = crate::storage::create_metadata_batch(&doc.id, &doc.text)?;
        self.insert(&key, metadata_batch, doc.vector.clone())?;

        if let Ok(mut cache) = self.doc_cache.lock() {
            cache.insert(key, doc.clone());
        }
        
        info!(id = %doc.id, collection_id = %collection_id, "Document updated successfully");
        Ok(())
    }

    /// Delete by ID from NoSQL (JSON) + synced trees (for unified cleanup)
    #[instrument(skip(self), fields(collection_id, doc_id))]
    pub fn delete_doc(&self, collection_id: &str, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, doc_id = %id, "Deleting document");
        
        let key = format!("{}/{}", collection_id, id);
        self.doc_tree.remove(key.as_bytes())?;
        self.metadata_tree.remove(key.as_bytes())?;
        self.vector_tree.remove(key.as_bytes())?;
        
        if let Ok(mut cache) = self.doc_cache.lock() {
            cache.remove(&key);
        }
        
        info!(key = %key, "Document deleted successfully");
        Ok(())
    }

    /// Delete an entire collection and its documents
    #[instrument(skip(self), fields(env_id, col_id))]
    pub fn delete_collection(&self, env_id: &str, col_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(env_id = %env_id, col_id = %col_id, "Deleting collection");
        
        // 1. Remove all docs in collection from doc_tree, metadata_tree, vector_tree
        let prefix = format!("{}/", col_id);
        let mut deleted_count = 0;
        
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
            deleted_count += 1;
        }

        // 2. Remove collection metadata
        self.collection_tree.remove(col_id.as_bytes())?;

        // 3. Update environment to remove collection ID
        if let Some(mut env) = self.get_environment(env_id)? {
            env.collections.retain(|id| id != col_id);
            self.update_environment(env)?;
        }
        
        info!(col_id = %col_id, deleted_docs = deleted_count, "Collection deleted successfully");
        Ok(())
    }
}

// === RAG-specific storage methods ===

/// RAG-specific document stored in the database
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RagStorageDocument {
    /// Unique identifier
    pub id: String,
    /// Original document ID (for chunk grouping)
    pub doc_id: String,
    /// Text content
    pub text: String,
    /// Vector embedding
    pub embedding: Vec<f32>,
    /// Chunk index
    pub chunk_index: usize,
    /// Total chunks in document
    pub total_chunks: usize,
    /// Source identifier
    pub source: Option<String>,
    /// Creation timestamp
    pub created_at: String,
    /// Custom metadata
    pub metadata: serde_json::Value,
}

impl Storage {
    /// Insert a RAG document chunk
    #[instrument(skip(self, doc), fields(id = %doc.id, collection_id))]
    pub fn insert_rag_doc(
        &self,
        doc: &RagStorageDocument,
        collection_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(id = %doc.id, collection_id = %collection_id, "Inserting RAG document");
        
        // Serialize to JSON
        let json_bytes = serde_json::to_vec(doc)?;
        let key = format!("{}/{}", collection_id, doc.id);
        
        // Store in RAG tree
        self.rag_tree.insert(key.as_bytes(), json_bytes)?;
        
        // Also store in doc_tree and vector_tree for compatibility with existing search
        let storage_doc = Document {
            id: doc.id.clone(),
            text: doc.text.clone(),
            category: "rag".to_string(),
            vector: doc.embedding.clone(),
            metadata: serde_json::json!({
                "doc_id": doc.doc_id,
                "chunk_index": doc.chunk_index,
                "total_chunks": doc.total_chunks,
                "source": doc.source,
                "created_at": doc.created_at,
                "custom": doc.metadata,
            }),
        };
        self.insert_doc(storage_doc, collection_id)?;
        
        info!(id = %doc.id, collection_id = %collection_id, "RAG document inserted");
        Ok(())
    }

    /// Get a RAG document by ID
    #[instrument(skip(self), fields(collection_id, doc_id))]
    pub fn get_rag_doc(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<RagStorageDocument, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, doc_id = %doc_id, "Getting RAG document");
        
        let key = format!("{}/{}", collection_id, doc_id);
        
        if let Some(doc_bytes) = self.rag_tree.get(key.as_bytes())? {
            let doc: RagStorageDocument = serde_json::from_slice(&doc_bytes)?;
            Ok(doc)
        } else {
            warn!(key = %key, "RAG document not found");
            Err("RAG document not found".into())
        }
    }

    /// Get all chunks for a document
    #[instrument(skip(self), fields(collection_id, doc_id))]
    pub fn get_rag_doc_chunks(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<RagStorageDocument>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, doc_id = %doc_id, "Getting RAG document chunks");
        
        let prefix = format!("{}/{}-", collection_id, doc_id);
        let mut chunks = Vec::new();
        
        for item in self.rag_tree.scan_prefix(prefix.as_bytes()) {
            let (_, v) = item?;
            let doc: RagStorageDocument = serde_json::from_slice(&v)?;
            chunks.push(doc);
        }
        
        // Also check for single-chunk document
        let single_key = format!("{}/{}", collection_id, doc_id);
        if let Some(doc_bytes) = self.rag_tree.get(single_key.as_bytes())? {
            let doc: RagStorageDocument = serde_json::from_slice(&doc_bytes)?;
            if !chunks.contains(&doc) {
                chunks.push(doc);
            }
        }
        
        // Sort by chunk index
        chunks.sort_by_key(|d| d.chunk_index);
        
        info!(collection_id = %collection_id, doc_id = %doc_id, chunks = chunks.len(), "RAG chunks retrieved");
        Ok(chunks)
    }

    /// Delete all chunks for a RAG document
    #[instrument(skip(self), fields(collection_id, doc_id))]
    pub fn delete_rag_doc(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, doc_id = %doc_id, "Deleting RAG document");
        
        // Get all chunks first
        let chunks = self.get_rag_doc_chunks(collection_id, doc_id)?;
        
        let deleted_count = chunks.len();

        // Delete each chunk
        for chunk in chunks {
            let key = format!("{}/{}", collection_id, chunk.id);
            self.rag_tree.remove(key.as_bytes())?;
            
            // Also delete from doc_tree and vector_tree
            self.doc_tree.remove(key.as_bytes())?;
            self.metadata_tree.remove(key.as_bytes())?;
            self.vector_tree.remove(key.as_bytes())?;
            
            // Remove from cache
            if let Ok(mut cache) = self.doc_cache.lock() {
                cache.remove(&key);
            }
        }
        
        info!(collection_id = %collection_id, doc_id = %doc_id, chunks_deleted = deleted_count, "RAG document deleted");
        Ok(())
    }

    /// Get all RAG documents in a collection
    #[instrument(skip(self), fields(collection_id))]
    pub fn get_rag_docs_in_collection(
        &self,
        collection_id: &str,
    ) -> Result<Vec<RagStorageDocument>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Getting all RAG documents in collection");
        
        let prefix = format!("{}/", collection_id);
        let mut docs = Vec::new();
        
        for item in self.rag_tree.scan_prefix(prefix.as_bytes()) {
            let (_, v) = item?;
            let doc: RagStorageDocument = serde_json::from_slice(&v)?;
            docs.push(doc);
        }
        
        info!(collection_id = %collection_id, docs = docs.len(), "RAG documents retrieved");
        Ok(docs)
    }

    /// Get unique document IDs in a RAG collection
    #[instrument(skip(self), fields(collection_id))]
    pub fn get_rag_doc_ids(
        &self,
        collection_id: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Getting unique RAG document IDs");
        
        let docs = self.get_rag_docs_in_collection(collection_id)?;
        let mut unique_ids = std::collections::HashSet::new();
        
        for doc in docs {
            unique_ids.insert(doc.doc_id);
        }
        
        let ids: Vec<String> = unique_ids.into_iter().collect();
        info!(collection_id = %collection_id, unique_docs = ids.len(), "Unique RAG document IDs retrieved");
        Ok(ids)
    }

    /// Update a RAG document (delete old chunks, insert new ones)
    #[instrument(skip(self, chunks), fields(collection_id, doc_id))]
    pub fn update_rag_doc(
        &self,
        collection_id: &str,
        doc_id: &str,
        chunks: Vec<RagStorageDocument>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, doc_id = %doc_id, new_chunks = chunks.len(), "Updating RAG document");
        
        // Delete existing chunks
        self.delete_rag_doc(collection_id, doc_id)?;
        
        // Insert new chunks
        for chunk in chunks {
            self.insert_rag_doc(&chunk, collection_id)?;
        }
        
        info!(collection_id = %collection_id, doc_id = %doc_id, "RAG document updated");
        Ok(())
    }
}
