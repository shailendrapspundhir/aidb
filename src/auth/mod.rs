use bcrypt::{hash, verify, DEFAULT_COST};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey, Algorithm};
use crate::tenants::AuthPayload;
use crate::session::get_session_manager;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, instrument};

const SECRET_KEY: &[u8] = b"my_super_secret_key"; // In prod, use env var

#[instrument(skip(password))]
pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    debug!("Hashing password");
    hash(password, DEFAULT_COST)
}

#[instrument(skip(password, hash))]
pub fn verify_password(password: &str, hash: &str) -> Result<bool, bcrypt::BcryptError> {
    debug!("Verifying password");
    verify(password, hash)
}

#[instrument(skip(username))]
pub fn create_jwt(username: &str) -> Result<String, jsonwebtoken::errors::Error> {
    debug!(username = %username, "Creating JWT token");
    
    let expiration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize + 3600; // 1 hour

    let claims = AuthPayload {
        sub: username.to_owned(),
        exp: expiration,
        session_id: None,
    };

    encode(&Header::default(), &claims, &EncodingKey::from_secret(SECRET_KEY))
}

/// Create a JWT with a new session for the user
#[instrument(skip(username))]
pub fn create_jwt_with_session(username: &str) -> Result<(String, String), jsonwebtoken::errors::Error> {
    debug!(username = %username, "Creating JWT token with session");
    
    // Create a new session
    let session_manager = get_session_manager();
    let session_id = session_manager.create_session(username);
    
    info!(username = %username, session_id = %session_id, "New session created");
    
    let expiration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize + 3600; // 1 hour

    let claims = AuthPayload {
        sub: username.to_owned(),
        exp: expiration,
        session_id: Some(session_id.clone()),
    };

    let token = encode(&Header::default(), &claims, &EncodingKey::from_secret(SECRET_KEY))?;
    Ok((token, session_id))
}

#[instrument(skip(token))]
pub fn validate_jwt(token: &str) -> Result<AuthPayload, jsonwebtoken::errors::Error> {
    debug!("Validating JWT token");
    
    let token_data = decode::<AuthPayload>(
        token,
        &DecodingKey::from_secret(SECRET_KEY),
        &Validation::new(Algorithm::HS256),
    )?;
    
    debug!(username = %token_data.claims.sub, "JWT token validated successfully");
    Ok(token_data.claims)
}
