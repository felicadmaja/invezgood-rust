use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub user_id: String,
    pub email: String,
    pub name: String,
    #[serde(default)]
    pub role: String,
    pub exp: i64,
    pub iat: i64,
    pub jti: String,
}

fn secret() -> Result<String, String> {
    std::env::var("JWT_SECRET").map_err(|_| {
        "JWT_SECRET wajib diisi di .env (shared secret untuk encode/decode JWT)".to_string()
    })
}

pub fn expiry_secs() -> i64 {
    std::env::var("JWT_EXPIRY_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(86_400)
}

pub fn encode_token(
    user_id: &Uuid,
    email: &str,
    name: &str,
    role: &str,
) -> Result<(String, i64), Box<dyn std::error::Error + Send + Sync>> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    let expires_in = expiry_secs();
    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        sub: email.to_string(),
        user_id: user_id.to_string(),
        email: email.to_string(),
        name: name.to_string(),
        role: role.to_string(),
        iat: now,
        exp: now + expires_in,
        jti: Uuid::new_v4().to_string(),
    };

    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret()?.as_bytes()),
    )?;
    Ok((token, expires_in))
}

pub fn decode_token(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

    let secret = std::env::var("JWT_SECRET").map_err(|_| {
        jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken)
    })?;

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?;
    Ok(data.claims)
}
