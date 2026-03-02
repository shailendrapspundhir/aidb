use crate::storage::Storage;
use crate::tenants::{Collection, Environment, Tenant, User};
use serde_json;

impl Storage {
    // User CRUD
    pub fn create_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        let key = user.username.as_bytes();
        if self.user_tree.contains_key(key)? {
            return Err("User already exists".into());
        }
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(key, value)?;
        Ok(())
    }

    pub fn get_user(&self, username: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
        match self.user_tree.get(username.as_bytes())? {
            Some(value) => {
                let user: User = serde_json::from_slice(&value)?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    pub fn update_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(user.username.as_bytes(), value)?;
        Ok(())
    }

    // Tenant CRUD
    pub fn create_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_tenant(&self, id: &str) -> Result<Option<Tenant>, Box<dyn std::error::Error>> {
        match self.tenant_tree.get(id.as_bytes())? {
            Some(value) => {
                let tenant: Tenant = serde_json::from_slice(&value)?;
                Ok(Some(tenant))
            }
            None => Ok(None),
        }
    }

    pub fn update_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        Ok(())
    }

    // Environment CRUD
    pub fn create_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_environment(&self, id: &str) -> Result<Option<Environment>, Box<dyn std::error::Error>> {
        match self.env_tree.get(id.as_bytes())? {
            Some(value) => {
                let env: Environment = serde_json::from_slice(&value)?;
                Ok(Some(env))
            }
            None => Ok(None),
        }
    }

    pub fn update_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        Ok(())
    }

    // Collection CRUD
    pub fn create_collection(&self, col: Collection) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(&col)?;
        self.collection_tree.insert(col.id.as_bytes(), value)?;
        Ok(())
    }

    pub fn get_collection(&self, id: &str) -> Result<Option<Collection>, Box<dyn std::error::Error>> {
        match self.collection_tree.get(id.as_bytes())? {
            Some(value) => {
                let col: Collection = serde_json::from_slice(&value)?;
                Ok(Some(col))
            }
            None => Ok(None),
        }
    }
}
