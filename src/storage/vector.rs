use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{info, debug, warn, error, instrument};

use crate::storage::Storage;

impl Storage {
    /// Insert an Arrow RecordBatch (metadata) and a vector for a given ID
    #[instrument(skip(self, metadata_batch, vector), fields(id))]
    pub fn insert(
        &self,
        id: &str,
        metadata_batch: RecordBatch,
        vector: Vec<f32>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(id = %id, vector_len = vector.len(), "Inserting vector and metadata");
        
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
        
        debug!(id = %id, "Vector and metadata inserted successfully");
        Ok(())
    }

    /// Retrieve Arrow RecordBatch (metadata) and vector by ID
    /// (Legacy vector-specific getter; see get_doc for NoSQL)
    #[instrument(skip(self), fields(id))]
    pub fn get(
        &self,
        id: &str,
    ) -> Result<(RecordBatch, Vec<f32>), Box<dyn std::error::Error>> {
        debug!(id = %id, "Retrieving vector and metadata");
        
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
                debug!(id = %id, vector_len = vector.len(), "Vector and metadata retrieved");
                Ok((batch, vector))
            } else {
                warn!(id = %id, "Vector not found");
                Err("Vector not found".into())
            }
        } else {
            warn!(id = %id, "Metadata not found");
            Err("Metadata not found".into())
        }
    }

    /// Get all vectors for indexing purposes (returns id and vector)
    #[instrument(skip(self))]
    pub fn get_vectors_in_collection(&self, collection_id: &str) -> Result<Vec<(String, Vec<f32>)>, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Retrieving all vectors in collection");
        
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
        
        info!(collection_id = %collection_id, count = vectors.len(), "Vectors retrieved");
        Ok(vectors)
    }
}

/// Helper to create a sample metadata RecordBatch for an item
#[instrument(skip(id, text))]
pub fn create_metadata_batch(id: &str, text: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    debug!(id = %id, "Creating metadata batch");
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
    ]));

    let id_array = StringArray::from(vec![id]);
    let text_array = StringArray::from(vec![text]);

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(text_array),
        ],
    )?)
}
