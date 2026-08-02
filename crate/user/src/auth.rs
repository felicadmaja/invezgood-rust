use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use getrandom::getrandom;
use tokio::sync::RwLock;

use crate::model::UserRow;

/// Panjang token session: 32 byte random → 64 karakter hex (256-bit entropy).
const SESSION_TOKEN_BYTES: usize = 32;

/// Satu sesi login berlaku 12 jam.
pub const SESSION_EXPIRES_SECS: i64 = 12 * 60 * 60;

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub nama: String,
    pub role: String,
    pub expires_at: DateTime<Utc>,
}

pub type SessionStore = Arc<RwLock<HashMap<String, AuthSession>>>;

pub fn new_session_store() -> SessionStore {
    Arc::new(RwLock::new(HashMap::new()))
}

fn generate_session_token() -> Result<String, String> {
    let mut bytes = [0u8; SESSION_TOKEN_BYTES];
    getrandom(&mut bytes).map_err(|e| format!("generate session token gagal: {e}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub async fn login(
    store: &SessionStore,
    user: UserRow,
    password: &str,
) -> Result<(String, AuthSession), String> {
    let stored_hash = user.password.as_deref().unwrap_or_default();

    let valid = crate::password::verify_password(password, stored_hash)
        .map_err(|e| format!("verifikasi password gagal: {e}"))?;

    if !valid {
        return Err("email atau password salah".into());
    }

    let auth = AuthSession {
        nama: user.nama.unwrap_or_default(),
        role: user.role.unwrap_or_default(),
        expires_at: Utc::now() + Duration::seconds(SESSION_EXPIRES_SECS),
    };

    let token = generate_session_token()?;
    store.write().await.insert(token.clone(), auth.clone());

    Ok((token, auth))
}

pub async fn logout(store: &SessionStore, token: &str) -> bool {
    store.write().await.remove(token).is_some()
}

pub async fn validate_session(store: &SessionStore, token: &str) -> Result<AuthSession, String> {
    let mut guard = store.write().await;
    let Some(auth) = guard.get(token).cloned() else {
        return Err("session tidak valid atau sudah logout".into());
    };

    if Utc::now() > auth.expires_at {
        guard.remove(token);
        return Err("session sudah expired".into());
    }

    Ok(auth)
}

/// Ambil token login dari metadata gRPC: `authorization: Bearer <token>`.
pub fn extract_bearer_token<T>(request: &tonic::Request<T>) -> Result<String, tonic::Status> {
    let metadata = request.metadata();
    let auth = metadata
        .get("authorization")
        .ok_or_else(|| tonic::Status::unauthenticated("authorization header wajib"))?
        .to_str()
        .map_err(|_| tonic::Status::unauthenticated("authorization header tidak valid"))?;

    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))
        .ok_or_else(|| {
            tonic::Status::unauthenticated("format authorization: Bearer <token>")
        })?
        .trim();

    if token.is_empty() {
        return Err(tonic::Status::unauthenticated("token wajib diisi"));
    }

    Ok(token.to_string())
}
