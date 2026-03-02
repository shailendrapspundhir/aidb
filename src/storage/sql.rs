use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{info, debug, warn, instrument};

use crate::storage::{Document, Storage};

impl Storage {
    /// Project NoSQL docs from Sled into Arrow RecordBatch
    /// This is the hybrid link: Enables SQL queries via DataFusion on
    /// structured view of JSON data (high-perf vectorized scans).
    /// Supports push-down filters for category, text, etc.
    /// Fixed schema: basic columns to ensure DataFusion table register/query success
    /// (vector stringified for hybrid; full List for prod).
    #[instrument(skip(self))]
    pub fn project_collection_to_arrow(&self, collection_id: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        debug!(collection_id = %collection_id, "Projecting collection to Arrow");
        
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
            debug!(collection_id = %collection_id, "No documents found, creating empty batch");
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

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(text_array) as ArrayRef,
                Arc::new(cat_array) as ArrayRef,
                Arc::new(vec_str_array) as ArrayRef,
            ],
        )?;
        
        info!(collection_id = %collection_id, rows = batch.num_rows(), "Collection projected to Arrow");
        Ok(batch)
    }
}
