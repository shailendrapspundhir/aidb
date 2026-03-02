use serde::{Deserialize, Serialize};

/// DTO for user registration
#[derive(Deserialize)]
pub struct UserRegister {
    pub username: String,
    pub password: String,
}

/// DTO for user login
#[derive(Deserialize)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

/// DTO for login response
#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
}

/// DTO for NoSQL JSON insert (REST body)
#[derive(Deserialize)]
pub struct InsertDocRest {
    pub id: String,
    pub text: String,
    pub category: String,
    pub vector: Vec<f32>,
    pub metadata_json: String,  // Flexible NoSQL JSON
}

/// DTO for update (same as insert)
pub type UpdateDocRest = InsertDocRest;

/// Generic REST response (JSON)
#[derive(Serialize)]
pub struct RestResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<String>,  // IDs or query results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hits: Option<Vec<bool>>, // True if fetched from cache
}

/// DTO for creating a tenant
#[derive(Deserialize)]
pub struct CreateTenantRest {
    pub id: String,
    pub name: String,
}

/// DTO for creating an environment
#[derive(Deserialize)]
pub struct CreateEnvRest {
    pub id: String,
    pub name: String,
}

/// DTO for creating a collection
#[derive(Deserialize)]
pub struct CreateCollectionRest {
    pub id: String,
    pub name: String,
}

/// DTO for SQL REST requests
#[derive(Deserialize)]
pub struct SqlRest {
    pub sql: String,
}

/// DTO for hybrid search REST requests
#[derive(Deserialize)]
pub struct HybridRest {
    pub sql_filter: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}