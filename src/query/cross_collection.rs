use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use crate::storage::Storage;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Left,
    Inner,
    Right,
    Full,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LookupStage {
    pub from: String,
    pub local_field: String,
    pub foreign_field: String,
    pub r#as: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinStage {
    pub join_type: JoinType,
    pub from: String,
    pub local_field: String,
    pub foreign_field: String,
    pub r#as: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnionStage {
    pub collections: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiCollectionOperation {
    pub operation: MultiCollectionOpType,
    pub target_collections: Vec<String>,
    pub documents: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiCollectionOpType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossCollectionStage {
    Lookup(LookupStage),
    Join(JoinStage),
    Union(UnionStage),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CrossCollectionPipeline {
    pub source_collection: String,
    pub stages: Vec<CrossCollectionStage>,
}

pub struct CrossCollectionEngine {
    storage: Arc<Storage>,
}

impl CrossCollectionEngine {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    #[instrument(skip(self, pipeline))]
    pub fn execute(
        &self,
        pipeline: CrossCollectionPipeline,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        debug!(
            source = %pipeline.source_collection,
            stages = pipeline.stages.len(),
            "Executing cross-collection pipeline"
        );

        let mut docs: Vec<Value> = self
            .storage
            .get_docs_in_collection(&pipeline.source_collection)?
            .into_iter()
            .map(|doc| {
                json!({
                    "id": doc.id,
                    "text": doc.text,
                    "category": doc.category,
                    "vector": doc.vector,
                    "metadata": doc.metadata,
                })
            })
            .collect();

        for stage in pipeline.stages {
            docs = match stage {
                CrossCollectionStage::Lookup(lookup) => self.apply_lookup(docs, lookup)?,
                CrossCollectionStage::Join(join) => self.apply_join(docs, join)?,
                CrossCollectionStage::Union(union) => self.apply_union(docs, union)?,
            };
        }

        info!(
            source = %pipeline.source_collection,
            result_count = docs.len(),
            "Cross-collection pipeline completed"
        );

        Ok(docs)
    }

    fn apply_lookup(
        &self,
        docs: Vec<Value>,
        lookup: LookupStage,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let foreign_docs: Vec<Value> = self
            .storage
            .get_docs_in_collection(&lookup.from)?
            .into_iter()
            .map(|doc| {
                json!({
                    "id": doc.id,
                    "text": doc.text,
                    "category": doc.category,
                    "vector": doc.vector,
                    "metadata": doc.metadata,
                })
            })
            .collect();

        let mut results = Vec::new();
        for doc in docs {
            let mut doc = doc.clone();
            let local_value = get_nested_value(&doc, &lookup.local_field);

            let matched: Vec<Value> = foreign_docs
                .iter()
                .filter(|fdoc| {
                    let foreign_value = get_nested_value(fdoc, &lookup.foreign_field);
                    local_value.is_some()
                        && foreign_value.is_some()
                        && local_value == foreign_value
                })
                .cloned()
                .collect();

            if let Some(map) = doc.as_object_mut() {
                map.insert(lookup.r#as.clone(), Value::Array(matched));
            }
            results.push(doc);
        }

        Ok(results)
    }

    fn apply_join(
        &self,
        docs: Vec<Value>,
        join: JoinStage,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let foreign_docs: Vec<Value> = self
            .storage
            .get_docs_in_collection(&join.from)?
            .into_iter()
            .map(|doc| {
                json!({
                    "id": doc.id,
                    "text": doc.text,
                    "category": doc.category,
                    "vector": doc.vector,
                    "metadata": doc.metadata,
                })
            })
            .collect();

        let mut results = Vec::new();

        for local_doc in &docs {
            let local_value = get_nested_value(local_doc, &join.local_field);
            let mut joined = false;

            for foreign_doc in &foreign_docs {
                let foreign_value = get_nested_value(foreign_doc, &join.foreign_field);

                if local_value.is_some()
                    && foreign_value.is_some()
                    && local_value == foreign_value
                {
                    let mut merged = local_doc.clone();
                    merge_docs(&mut merged, foreign_doc, &join.r#as);
                    results.push(merged);
                    joined = true;
                }
            }

            if !joined {
                match join.join_type {
                    JoinType::Left | JoinType::Full => {
                        let mut merged = local_doc.clone();
                        if let Some(map) = merged.as_object_mut() {
                            map.insert(join.r#as.clone(), Value::Null);
                        }
                        results.push(merged);
                    }
                    _ => {}
                }
            }
        }

        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for foreign_doc in &foreign_docs {
                let foreign_value = get_nested_value(foreign_doc, &join.foreign_field);
                let mut found = false;

                for local_doc in &docs {
                    let local_value = get_nested_value(local_doc, &join.local_field);
                    if local_value.is_some()
                        && foreign_value.is_some()
                        && local_value == foreign_value
                    {
                        found = true;
                        break;
                    }
                }

                if !found {
                    let mut merged = foreign_doc.clone();
                    merge_docs(&mut merged, &json!({}), &"_local");
                    results.push(merged);
                }
            }
        }

        Ok(results)
    }

    fn apply_union(
        &self,
        docs: Vec<Value>,
        union: UnionStage,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let mut all_docs = docs;

        for collection in &union.collections {
            let collection_docs: Vec<Value> = self
                .storage
                .get_docs_in_collection(collection)?
                .into_iter()
                .map(|doc| {
                    json!({
                        "id": doc.id,
                        "text": doc.text,
                        "category": doc.category,
                        "vector": doc.vector,
                        "metadata": doc.metadata,
                        "_source_collection": collection.clone(),
                    })
                })
                .collect();
            all_docs.extend(collection_docs);
        }

        Ok(all_docs)
    }

    pub fn execute_multi_collection_operation(
        &self,
        operation: MultiCollectionOperation,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();

        match operation.operation {
            MultiCollectionOpType::Insert => {
                for (idx, doc) in operation.documents.iter().enumerate() {
                    let collection_idx = idx % operation.target_collections.len();
                    let collection = &operation.target_collections[collection_idx];

                    let id = doc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let text = doc.get("text").and_then(|v| v.as_str()).unwrap_or("");
                    let category = doc.get("category").and_then(|v| v.as_str()).unwrap_or("");
                    let vector = doc.get("vector").cloned().unwrap_or(json!([]));
                    let metadata = doc.get("metadata").cloned().unwrap_or(json!({}));

                    let document = crate::storage::Document {
                        id: id.to_string(),
                        text: text.to_string(),
                        category: category.to_string(),
                        vector: serde_json::from_value(vector).unwrap_or_default(),
                        metadata,
                    };

                    self.storage.insert_doc(document, collection)?;
                    results.push(format!("{}/{}: inserted", collection, id));
                }
            }
            MultiCollectionOpType::Update => {
                for doc in &operation.documents {
                    for collection in &operation.target_collections {
                        if let Some(id) = doc.get("id").and_then(|v| v.as_str()) {
                            if let Ok(existing) = self.storage.get_doc(collection, id) {
                                let updated = crate::storage::Document {
                                    id: id.to_string(),
                                    text: doc.get("text").and_then(|v| v.as_str()).unwrap_or(&existing.text).to_string(),
                                    category: doc.get("category").and_then(|v| v.as_str()).unwrap_or(&existing.category).to_string(),
                                    vector: doc.get("vector")
                                        .and_then(|v| serde_json::from_value(v.clone()).ok())
                                        .unwrap_or(existing.vector),
                                    metadata: doc.get("metadata").cloned().unwrap_or(existing.metadata),
                                };
                                self.storage.update_doc(updated, collection)?;
                                results.push(format!("{}/{}: updated", collection, id));
                            }
                        }
                    }
                }
            }
            MultiCollectionOpType::Delete => {
                for doc in &operation.documents {
                    for collection in &operation.target_collections {
                        if let Some(id) = doc.get("id").and_then(|v| v.as_str()) {
                            self.storage.delete_doc(collection, id)?;
                            results.push(format!("{}/{}: deleted", collection, id));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

fn get_nested_value(doc: &Value, field: &str) -> Option<Value> {
    let mut current = doc;
    for part in field.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

fn merge_docs(target: &mut Value, source: &Value, prefix: &str) {
    if let (Some(target_map), Some(source_map)) = (target.as_object_mut(), source.as_object()) {
        for (key, value) in source_map {
            let new_key = format!("{}.{}", prefix, key);
            target_map.insert(new_key, value.clone());
        }
    }
}

impl CrossCollectionPipeline {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let obj = value.as_object().ok_or("Pipeline must be a JSON object")?;

        let source_collection = obj
            .get("source")
            .and_then(|v| v.as_str())
            .ok_or("Missing source collection")?
            .to_string();

        let stages_value = obj
            .get("stages")
            .and_then(|v| v.as_array())
            .ok_or("Missing stages array")?;

        let mut stages = Vec::new();
        for stage_value in stages_value {
            let stage = parse_cross_collection_stage(stage_value)?;
            stages.push(stage);
        }

        Ok(Self {
            source_collection,
            stages,
        })
    }
}

fn parse_cross_collection_stage(
    value: &Value,
) -> Result<CrossCollectionStage, Box<dyn std::error::Error>> {
    let obj = value
        .as_object()
        .ok_or("Stage must be a JSON object")?;

    if let Some(lookup_value) = obj.get("lookup") {
        let lookup: LookupStage = serde_json::from_value(lookup_value.clone())?;
        return Ok(CrossCollectionStage::Lookup(lookup));
    }
    if let Some(join_value) = obj.get("join") {
        let join: JoinStage = serde_json::from_value(join_value.clone())?;
        return Ok(CrossCollectionStage::Join(join));
    }
    if let Some(union_value) = obj.get("union") {
        let union: UnionStage = serde_json::from_value(union_value.clone())?;
        return Ok(CrossCollectionStage::Union(union));
    }

    Err("Unsupported cross-collection stage".into())
}

impl MultiCollectionOperation {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let obj = value.as_object().ok_or("Operation must be a JSON object")?;

        let operation = obj
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or("Missing operation type")?;

        let op_type = match operation {
            "insert" => MultiCollectionOpType::Insert,
            "update" => MultiCollectionOpType::Update,
            "delete" => MultiCollectionOpType::Delete,
            _ => return Err("Invalid operation type".into()),
        };

        let target_collections = obj
            .get("collections")
            .and_then(|v| v.as_array())
            .ok_or("Missing collections array")?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let documents = obj
            .get("documents")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(Self {
            operation: op_type,
            target_collections,
            documents,
        })
    }
}
