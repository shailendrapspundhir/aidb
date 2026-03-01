use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub tenants: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub owner_id: String,
    pub environments: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Environment {
    pub id: String,
    pub name: String,
    pub tenant_id: String,
    pub collections: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Collection {
    pub id: String,
    pub name: String,
    pub environment_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuthPayload {
    pub sub: String, // username
    pub exp: usize,
}
