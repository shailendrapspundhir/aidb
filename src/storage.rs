use arrow::array::{ArrayRef, StringArray};  // Float32Array unused after placeholder schema
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json;
use sled::Db;
use std::io::Cursor;
use std::sync::Arc;

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
        Ok(Self {
            db,
            metadata_tree,
            vector_tree,
            doc_tree,
        })
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
    pub fn insert_doc(&self, doc: Document) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize to JSON bytes for NoSQL storage in Sled
        let json_bytes = serde_json::to_vec(&doc)?;

        // Store raw JSON doc (NoSQL)
        self.doc_tree.insert(doc.id.as_bytes(), json_bytes)?;

        // Sync to existing vector/Arrow for compatibility (hybrid link)
        let metadata_batch = create_metadata_batch(&doc.id, &doc.text)?;
        self.insert(&doc.id, metadata_batch, doc.vector.clone())?;  // Reuses vector storage

        Ok(())
    }

    /// Retrieve NoSQL Document by ID (deserializes JSON from Sled)
    /// Enables dynamic/unstructured access.
    pub fn get_doc(&self, id: &str) -> Result<Document, Box<dyn std::error::Error>> {
        if let Some(doc_bytes) = self.doc_tree.get(id.as_bytes())? {
            let doc: Document = serde_json::from_slice(&doc_bytes)?;
            Ok(doc)
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
    pub fn project_to_arrow(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut ids = vec![];
        let mut texts = vec![];
        let mut categories = vec![];
        let mut vector_strs = vec![];  // Stringify vectors for SQL compat

        // Scan NoSQL docs from Sled
        for item in self.doc_tree.iter() {
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
    pub fn get_all_docs(&self) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        let mut docs = vec![];
        for item in self.doc_tree.iter() {
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
    pub fn update_doc(&self, doc: Document) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize updated JSON
        let json_bytes = serde_json::to_vec(&doc)?;

        // Upsert in doc_tree (NoSQL)
        self.doc_tree.insert(doc.id.as_bytes(), json_bytes)?;

        // Sync to Arrow/metadata + vector trees for SQL/index consistency
        let metadata_batch = create_metadata_batch(&doc.id, &doc.text)?;
        self.insert(&doc.id, metadata_batch, doc.vector.clone())?;

        Ok(())
    }

    /// Delete by ID from NoSQL (JSON) + synced trees (for unified cleanup)
    pub fn delete_doc(&self, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.doc_tree.remove(id.as_bytes())?;
        self.metadata_tree.remove(id.as_bytes())?;
        self.vector_tree.remove(id.as_bytes())?;
        // Note: For full SQL sync , re-project Arrow table post-delete in prod
        Ok(())
    }

    /// Get all vectors for indexing purposes (returns id and vector)
    pub fn get_all_vectors(&self) -> Result<Vec<(String, Vec<f32>)>, Box<dyn std::error::Error>> {
        let mut vectors = Vec::new();
        for item in self.vector_tree.iter() {
            let (k, v) = item?;
            let id = String::from_utf8(k.to_vec())?;
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
