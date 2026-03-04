use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// DTO for user registration
#[derive(Deserialize, ToSchema)]
pub struct UserRegister {
    pub username: String,
    pub password: String,
}

/// DTO for user login
#[derive(Deserialize, ToSchema)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

/// DTO for login response
#[derive(Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
}

/// DTO for NoSQL JSON insert (REST body)
#[derive(Deserialize, ToSchema, Clone)]
pub struct InsertDocRest {
    pub id: String,
    pub text: String,
    pub category: String,
    pub vector: Vec<f32>,
    pub metadata_json: String,  // Flexible NoSQL JSON
}

/// DTO for batch NoSQL JSON insert
#[derive(Deserialize, ToSchema)]
pub struct BatchInsertDocRest {
    pub documents: Vec<InsertDocRest>,
}

/// DTO for update (same as insert)
pub type UpdateDocRest = InsertDocRest;

/// Generic REST response (JSON)
#[derive(Serialize, ToSchema)]
pub struct RestResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<String>,  // IDs or query results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hits: Option<Vec<bool>>, // True if fetched from cache
}

/// DTO for creating a tenant
#[derive(Deserialize, ToSchema)]
pub struct CreateTenantRest {
    pub id: String,
    pub name: String,
}

/// DTO for creating an environment
#[derive(Deserialize, ToSchema)]
pub struct CreateEnvRest {
    pub id: String,
    pub name: String,
}

/// DTO for creating a collection
#[derive(Deserialize, ToSchema)]
pub struct CreateCollectionRest {
    pub id: String,
    pub name: String,
}

/// DTO for SQL REST requests
#[derive(Deserialize, ToSchema)]
pub struct SqlRest {
    pub sql: String,
}

/// DTO for hybrid search REST requests
#[derive(Deserialize, ToSchema)]
pub struct HybridRest {
    pub sql_filter: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}

/// DTO for full-text search REST requests
#[derive(Deserialize, ToSchema)]
pub struct TextSearchRest {
    pub query: String,
    pub partial_match: bool,
    pub case_sensitive: bool,
    pub include_metadata: bool,
}

/// DTO for full-text search responses
#[derive(Serialize, ToSchema)]
pub struct TextSearchResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<DocumentSummary>,
}

#[derive(Serialize, ToSchema)]
pub struct DocumentSummary {
    pub id: String,
    pub text: String,
    pub category: String,
}
