use crate::storage::Storage;
use crate::tenants::{Collection, Environment, Tenant, User};
use serde_json;
use tracing::{info, debug, warn, instrument};

impl Storage {
    // User CRUD
    #[instrument(skip(self, user), fields(username = %user.username))]
    pub fn create_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        debug!(username = %user.username, "Creating user");
        
        let key = user.username.as_bytes();
        if self.user_tree.contains_key(key)? {
            warn!(username = %user.username, "User already exists");
            return Err("User already exists".into());
        }
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(key, value)?;
        
        info!(username = %user.username, "User created successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(username))]
    pub fn get_user(&self, username: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
        debug!(username = %username, "Retrieving user");
        
        match self.user_tree.get(username.as_bytes())? {
            Some(value) => {
                let user: User = serde_json::from_slice(&value)?;
                debug!(username = %username, "User found");
                Ok(Some(user))
            }
            None => {
                debug!(username = %username, "User not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self, user), fields(username = %user.username))]
    pub fn update_user(&self, user: User) -> Result<(), Box<dyn std::error::Error>> {
        debug!(username = %user.username, "Updating user");
        
        let value = serde_json::to_vec(&user)?;
        self.user_tree.insert(user.username.as_bytes(), value)?;
        
        info!(username = %user.username, "User updated successfully");
        Ok(())
    }

    // Tenant CRUD
    #[instrument(skip(self, tenant), fields(tenant_id = %tenant.id))]
    pub fn create_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        debug!(tenant_id = %tenant.id, name = %tenant.name, "Creating tenant");
        
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        
        info!(tenant_id = %tenant.id, "Tenant created successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(tenant_id))]
    pub fn get_tenant(&self, id: &str) -> Result<Option<Tenant>, Box<dyn std::error::Error>> {
        debug!(tenant_id = %id, "Retrieving tenant");
        
        match self.tenant_tree.get(id.as_bytes())? {
            Some(value) => {
                let tenant: Tenant = serde_json::from_slice(&value)?;
                debug!(tenant_id = %id, "Tenant found");
                Ok(Some(tenant))
            }
            None => {
                debug!(tenant_id = %id, "Tenant not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self, tenant), fields(tenant_id = %tenant.id))]
    pub fn update_tenant(&self, tenant: Tenant) -> Result<(), Box<dyn std::error::Error>> {
        debug!(tenant_id = %tenant.id, "Updating tenant");
        
        let value = serde_json::to_vec(&tenant)?;
        self.tenant_tree.insert(tenant.id.as_bytes(), value)?;
        
        info!(tenant_id = %tenant.id, "Tenant updated successfully");
        Ok(())
    }

    // Environment CRUD
    #[instrument(skip(self, env), fields(env_id = %env.id))]
    pub fn create_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        debug!(env_id = %env.id, tenant_id = %env.tenant_id, "Creating environment");
        
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        
        info!(env_id = %env.id, "Environment created successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(env_id))]
    pub fn get_environment(&self, id: &str) -> Result<Option<Environment>, Box<dyn std::error::Error>> {
        debug!(env_id = %id, "Retrieving environment");
        
        match self.env_tree.get(id.as_bytes())? {
            Some(value) => {
                let env: Environment = serde_json::from_slice(&value)?;
                debug!(env_id = %id, "Environment found");
                Ok(Some(env))
            }
            None => {
                debug!(env_id = %id, "Environment not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self, env), fields(env_id = %env.id))]
    pub fn update_environment(&self, env: Environment) -> Result<(), Box<dyn std::error::Error>> {
        debug!(env_id = %env.id, "Updating environment");
        
        let value = serde_json::to_vec(&env)?;
        self.env_tree.insert(env.id.as_bytes(), value)?;
        
        info!(env_id = %env.id, "Environment updated successfully");
        Ok(())
    }

    // Collection CRUD
    #[instrument(skip(self, col), fields(collection_id = %col.id))]
    pub fn create_collection(&self, col: Collection) -> Result<(), Box<dyn std::error::Error>> {
        debug!(collection_id = %col.id, env_id = %col.environment_id, "Creating collection");
        
        let value = serde_json::to_vec(&col)?;
        self.collection_tree.insert(col.id.as_bytes(), value)?;
        
        info!(collection_id = %col.id, "Collection created successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(collection_id))]
    pub fn get_collection(&self, id: &str) -> Result<Option<Collection>, Box<dyn std::error::Error>> {
        debug!(collection_id = %id, "Retrieving collection");
        
        match self.collection_tree.get(id.as_bytes())? {
            Some(value) => {
                let col: Collection = serde_json::from_slice(&value)?;
                debug!(collection_id = %id, "Collection found");
                Ok(Some(col))
            }
            None => {
                debug!(collection_id = %id, "Collection not found");
                Ok(None)
            }
        }
    }
}
