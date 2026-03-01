use bcrypt::{hash, verify, DEFAULT_COST};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey, Algorithm};
use crate::models::AuthPayload;
use std::time::{SystemTime, UNIX_EPOCH};

const SECRET_KEY: &[u8] = b"my_super_secret_key"; // In prod, use env var

pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    hash(password, DEFAULT_COST)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, bcrypt::BcryptError> {
    verify(password, hash)
}

pub fn create_jwt(username: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let expiration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize + 3600; // 1 hour

    let claims = AuthPayload {
        sub: username.to_owned(),
        exp: expiration,
    };

    encode(&Header::default(), &claims, &EncodingKey::from_secret(SECRET_KEY))
}

pub fn validate_jwt(token: &str) -> Result<AuthPayload, jsonwebtoken::errors::Error> {
    let token_data = decode::<AuthPayload>(
        token,
        &DecodingKey::from_secret(SECRET_KEY),
        &Validation::new(Algorithm::HS256),
    )?;
    Ok(token_data.claims)
}
