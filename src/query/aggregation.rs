use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use crate::storage::Storage;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    In,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MatchFilter {
    pub field: String,
    pub op: MatchOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchLogic {
    And,
    Or,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MatchStage {
    pub filters: Vec<MatchFilter>,
    #[serde(default = "default_match_logic")]
    pub logic: MatchLogic,
}

fn default_match_logic() -> MatchLogic {
    MatchLogic::And
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SortField {
    pub field: String,
    #[serde(default = "default_sort_order")]
    pub order: SortOrder,
}

fn default_sort_order() -> SortOrder {
    SortOrder::Asc
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupAggregation {
    pub op: AggregationOp,
    #[serde(default)]
    pub field: Option<String>,
    pub r#as: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupStage {
    pub by: Vec<String>,
    pub aggregations: Vec<GroupAggregation>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProjectStage {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchStage {
    pub query: String,
    #[serde(default)]
    pub partial_match: bool,
    #[serde(default)]
    pub case_sensitive: bool,
    #[serde(default = "default_search_metadata")]
    pub include_metadata: bool,
}

fn default_search_metadata() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationStage {
    Match(MatchStage),
    Sort(Vec<SortField>),
    Group(GroupStage),
    Project(ProjectStage),
    Limit(usize),
    Skip(usize),
    Search(SearchStage),
    Lookup(LookupStage),
    Join(JoinStage),
    Union(UnionStage),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LookupStage {
    pub from: String,
    pub local_field: String,
    pub foreign_field: String,
    pub r#as: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Left,
    Inner,
    Right,
    Full,
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
pub struct AggregationPipeline {
    pub stages: Vec<AggregationStage>,
}

pub struct AggregationEngine {
    storage: Arc<Storage>,
    collection_id: String,
}

impl AggregationEngine {
    pub fn new(storage: Arc<Storage>, collection_id: &str) -> Self {
        Self {
            storage,
            collection_id: collection_id.to_string(),
        }
    }

    #[instrument(skip(self, pipeline))]
    pub fn execute(
        &self,
        pipeline: AggregationPipeline,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        debug!(
            collection_id = %self.collection_id,
            stages = pipeline.stages.len(),
            "Executing aggregation pipeline"
        );

        let mut docs: Vec<Value> = self
            .storage
            .get_docs_in_collection(&self.collection_id)?
            .into_iter()
            .map(|doc| {
                json!({
                    "id": doc.id,
                    "text": doc.text,
                    "category": doc.category,
                    "vector": doc.vector,
                    "metadata": doc.metadata,
                    "_source_collection": self.collection_id.clone(),
                })
            })
            .collect();

        for stage in pipeline.stages {
            docs = match stage {
                AggregationStage::Match(stage) => apply_match(docs, stage),
                AggregationStage::Sort(sort_fields) => apply_sort(docs, &sort_fields),
                AggregationStage::Group(stage) => apply_group(docs, stage),
                AggregationStage::Project(stage) => apply_project(docs, stage),
                AggregationStage::Limit(limit) => docs.into_iter().take(limit).collect(),
                AggregationStage::Skip(offset) => docs.into_iter().skip(offset).collect(),
                AggregationStage::Search(stage) => {
                    let results = self.storage.search_docs_text(
                        &self.collection_id,
                        &stage.query,
                        stage.partial_match,
                        stage.case_sensitive,
                        stage.include_metadata,
                    )?;
                    results
                        .into_iter()
                        .map(|doc| {
                            json!({
                                "id": doc.id,
                                "text": doc.text,
                                "category": doc.category,
                                "vector": doc.vector,
                                "metadata": doc.metadata,
                                "_source_collection": self.collection_id.clone(),
                            })
                        })
                        .collect()
                }
                AggregationStage::Lookup(lookup) => self.apply_lookup(docs, lookup)?,
                AggregationStage::Join(join) => self.apply_join(docs, join)?,
                AggregationStage::Union(union) => self.apply_union(docs, union)?,
            };
        }

        info!(
            collection_id = %self.collection_id,
            result_count = docs.len(),
            "Aggregation pipeline completed"
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
                    "_source_collection": lookup.from.clone(),
                })
            })
            .collect();

        let mut results = Vec::new();
        for doc in docs {
            let mut doc = doc.clone();
            let local_value = get_field_value(&doc, &lookup.local_field);

            let matched: Vec<Value> = foreign_docs
                .iter()
                .filter(|fdoc| {
                    let foreign_value = get_field_value(fdoc, &lookup.foreign_field);
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
                    "_source_collection": join.from.clone(),
                })
            })
            .collect();

        let mut results = Vec::new();

        for local_doc in &docs {
            let local_value = get_field_value(local_doc, &join.local_field);
            let mut joined = false;

            for foreign_doc in &foreign_docs {
                let foreign_value = get_field_value(foreign_doc, &join.foreign_field);

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
                let foreign_value = get_field_value(foreign_doc, &join.foreign_field);
                let mut found = false;

                for local_doc in &docs {
                    let local_value = get_field_value(local_doc, &join.local_field);
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
}

fn apply_match(docs: Vec<Value>, stage: MatchStage) -> Vec<Value> {
    docs.into_iter()
        .filter(|doc| match stage.logic {
            MatchLogic::And => stage
                .filters
                .iter()
                .all(|filter| evaluate_filter(doc, filter)),
            MatchLogic::Or => stage
                .filters
                .iter()
                .any(|filter| evaluate_filter(doc, filter)),
        })
        .collect()
}

fn apply_sort(mut docs: Vec<Value>, sort_fields: &[SortField]) -> Vec<Value> {
    docs.sort_by(|a, b| {
        for sort_field in sort_fields {
            let a_val = get_field_value(a, &sort_field.field);
            let b_val = get_field_value(b, &sort_field.field);
            let ordering = compare_values(a_val, b_val);
            if ordering != std::cmp::Ordering::Equal {
                return match sort_field.order {
                    SortOrder::Asc => ordering,
                    SortOrder::Desc => ordering.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
    });
    docs
}

fn apply_group(docs: Vec<Value>, stage: GroupStage) -> Vec<Value> {
    let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

    for doc in docs {
        let mut key_obj = Map::new();
        for field in &stage.by {
            let value = get_field_value(&doc, field).cloned().unwrap_or(Value::Null);
            key_obj.insert(field.clone(), value);
        }
        let key = Value::Object(key_obj.clone());
        groups
            .entry(key.to_string())
            .or_default()
            .push(doc);
    }

    let mut results = Vec::new();

    for (key, docs) in groups {
        let key_value: Value = serde_json::from_str(&key).unwrap_or(Value::Null);
        let mut output = Map::new();
        output.insert("_id".to_string(), key_value);

        for aggregation in &stage.aggregations {
            let value = apply_aggregation(&docs, aggregation);
            output.insert(aggregation.r#as.clone(), value);
        }

        results.push(Value::Object(output));
    }

    results
}

fn apply_project(docs: Vec<Value>, stage: ProjectStage) -> Vec<Value> {
    docs.into_iter()
        .map(|doc| project_document(doc, &stage.include, &stage.exclude))
        .collect()
}

fn evaluate_filter(doc: &Value, filter: &MatchFilter) -> bool {
    let Some(field_value) = get_field_value(doc, &filter.field) else {
        return false;
    };

    match filter.op {
        MatchOperator::Eq => field_value == &filter.value,
        MatchOperator::Ne => field_value != &filter.value,
        MatchOperator::Gt => compare_values(Some(field_value), Some(&filter.value))
            == std::cmp::Ordering::Greater,
        MatchOperator::Gte => {
            let ordering = compare_values(Some(field_value), Some(&filter.value));
            ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
        }
        MatchOperator::Lt => compare_values(Some(field_value), Some(&filter.value))
            == std::cmp::Ordering::Less,
        MatchOperator::Lte => {
            let ordering = compare_values(Some(field_value), Some(&filter.value));
            ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
        }
        MatchOperator::Contains => match field_value {
            Value::String(text) => filter.value.as_str().map_or(false, |val| text.contains(val)),
            Value::Array(values) => values.iter().any(|val| val == &filter.value),
            _ => false,
        },
        MatchOperator::In => match &filter.value {
            Value::Array(values) => values.iter().any(|val| val == field_value),
            _ => false,
        },
    }
}

fn get_field_value<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = doc;
    for part in field.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(Value::Number(a_num)), Some(Value::Number(b_num))) => {
            let a_float = a_num.as_f64().unwrap_or(0.0);
            let b_float = b_num.as_f64().unwrap_or(0.0);
            a_float.partial_cmp(&b_float).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Some(Value::String(a_str)), Some(Value::String(b_str))) => a_str.cmp(b_str),
        (Some(Value::Bool(a_bool)), Some(Value::Bool(b_bool))) => a_bool.cmp(b_bool),
        (Some(a_val), Some(b_val)) => a_val.to_string().cmp(&b_val.to_string()),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn apply_aggregation(docs: &[Value], aggregation: &GroupAggregation) -> Value {
    match aggregation.op {
        AggregationOp::Count => Value::Number(serde_json::Number::from(docs.len() as u64)),
        AggregationOp::Sum => aggregate_numeric(docs, aggregation.field.as_deref(), |vals| {
            vals.iter().sum()
        }),
        AggregationOp::Avg => aggregate_numeric(docs, aggregation.field.as_deref(), |vals| {
            if vals.is_empty() {
                0.0
            } else {
                vals.iter().sum::<f64>() / vals.len() as f64
            }
        }),
        AggregationOp::Min => aggregate_numeric(docs, aggregation.field.as_deref(), |vals| {
            vals.iter().cloned().fold(f64::INFINITY, f64::min)
        }),
        AggregationOp::Max => aggregate_numeric(docs, aggregation.field.as_deref(), |vals| {
            vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
        }),
    }
}

fn aggregate_numeric<F>(docs: &[Value], field: Option<&str>, op: F) -> Value
where
    F: FnOnce(Vec<f64>) -> f64,
{
    let mut values = Vec::new();
    if let Some(field) = field {
        for doc in docs {
            if let Some(value) = get_field_value(doc, field) {
                if let Some(num) = value.as_f64() {
                    values.push(num);
                }
            }
        }
    }

    let result = op(values);
    Value::Number(serde_json::Number::from_f64(result).unwrap_or_else(|| serde_json::Number::from(0)))
}

fn project_document(mut doc: Value, include: &[String], exclude: &[String]) -> Value {
    match &mut doc {
        Value::Object(map) => {
            if !include.is_empty() {
                let mut projected = Map::new();
                for field in include {
                    if let Some(value) = get_field_value(&doc, field).cloned() {
                        projected.insert(field.clone(), value);
                    }
                }
                return Value::Object(projected);
            }

            for field in exclude {
                remove_field(map, field);
            }
        }
        _ => {}
    }

    doc
}

fn remove_field(map: &mut Map<String, Value>, field: &str) {
    if !field.contains('.') {
        map.remove(field);
        return;
    }

    let mut parts = field.split('.').collect::<Vec<_>>();
    if let Some(last) = parts.pop() {
        let mut current = map;
        for part in parts {
            if let Some(Value::Object(next)) = current.get_mut(part) {
                current = next;
            } else {
                return;
            }
        }
        current.remove(last);
    }
}

fn merge_docs(target: &mut Value, source: &Value, prefix: &str) {
    if let (Some(target_map), Some(source_map)) = (target.as_object_mut(), source.as_object()) {
        for (key, value) in source_map {
            let new_key = format!("{}.{}", prefix, key);
            target_map.insert(new_key, value.clone());
        }
    }
}

impl AggregationPipeline {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let stages_value = value
            .as_array()
            .ok_or("Aggregation pipeline must be a JSON array")?;

        let mut stages = Vec::new();
        for stage_value in stages_value {
            let stage = parse_stage(stage_value)?;
            stages.push(stage);
        }

        Ok(Self { stages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregation_pipeline_with_group() {
        let pipeline_json = json!([
            {"match": {"filters": [{"field": "category", "op": "eq", "value": "AI"}], "logic": "and"}},
            {"group": {"by": ["category"], "aggregations": [{"op": "count", "as": "doc_count"}]}}
        ]);

        let pipeline = AggregationPipeline::from_value(pipeline_json).expect("pipeline should parse");
        assert_eq!(pipeline.stages.len(), 2);
        match &pipeline.stages[0] {
            AggregationStage::Match(stage) => assert_eq!(stage.filters.len(), 1),
            _ => panic!("expected match stage"),
        }
    }
}

fn parse_stage(value: &Value) -> Result<AggregationStage, Box<dyn std::error::Error>> {
    let obj = value
        .as_object()
        .ok_or("Aggregation stage must be a JSON object")?;

    if let Some(match_value) = obj.get("match") {
        let stage: MatchStage = serde_json::from_value(match_value.clone())?;
        return Ok(AggregationStage::Match(stage));
    }
    if let Some(sort_value) = obj.get("sort") {
        let stage: Vec<SortField> = serde_json::from_value(sort_value.clone())?;
        return Ok(AggregationStage::Sort(stage));
    }
    if let Some(group_value) = obj.get("group") {
        let stage: GroupStage = serde_json::from_value(group_value.clone())?;
        return Ok(AggregationStage::Group(stage));
    }
    if let Some(project_value) = obj.get("project") {
        let stage: ProjectStage = serde_json::from_value(project_value.clone())?;
        return Ok(AggregationStage::Project(stage));
    }
    if let Some(limit_value) = obj.get("limit") {
        let limit = limit_value
            .as_u64()
            .ok_or("limit stage must be a number")? as usize;
        return Ok(AggregationStage::Limit(limit));
    }
    if let Some(skip_value) = obj.get("skip") {
        let skip = skip_value
            .as_u64()
            .ok_or("skip stage must be a number")? as usize;
        return Ok(AggregationStage::Skip(skip));
    }
    if let Some(search_value) = obj.get("search") {
        let stage: SearchStage = serde_json::from_value(search_value.clone())?;
        return Ok(AggregationStage::Search(stage));
    }
    if let Some(lookup_value) = obj.get("lookup") {
        let stage: LookupStage = serde_json::from_value(lookup_value.clone())?;
        return Ok(AggregationStage::Lookup(stage));
    }
    if let Some(join_value) = obj.get("join") {
        let stage: JoinStage = serde_json::from_value(join_value.clone())?;
        return Ok(AggregationStage::Join(stage));
    }
    if let Some(union_value) = obj.get("union") {
        let stage: UnionStage = serde_json::from_value(union_value.clone())?;
        return Ok(AggregationStage::Union(stage));
    }

    Err("Unsupported aggregation stage".into())
}
