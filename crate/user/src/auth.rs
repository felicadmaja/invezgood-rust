use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Local, Timelike, Utc};
use getrandom::getrandom;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::model::UserRow;

/// Default JWT berlaku 30 hari (override via JWT_EXPIRY_SECS).
pub const DEFAULT_JWT_EXPIRY_SECS: i64 = 30 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub nama: String,
    pub role: String,
    pub expires_at: DateTime<Utc>,
}

/// Store sesi aktif: key = JWT `jti`. Logout menghapus jti → token di-revoke.
pub type SessionStore = Arc<RwLock<HashMap<String, AuthSession>>>;

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    /// Email user (subject).
    sub: String,
    nama: String,
    role: String,
    /// JWT ID — dipakai revoke saat logout.
    jti: String,
    exp: usize,
}

pub fn new_session_store() -> SessionStore {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn jwt_expiry_secs() -> i64 {
    std::env::var("JWT_EXPIRY_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|&secs| secs > 0)
        .unwrap_or(DEFAULT_JWT_EXPIRY_SECS)
}

fn jwt_secret() -> Result<String, String> {
    let secret = std::env::var("JWT_SECRET")
        .map_err(|_| "JWT_SECRET wajib diisi".to_string())?;
    if secret.trim().is_empty() {
        return Err("JWT_SECRET tidak boleh kosong".into());
    }
    Ok(secret)
}

fn generate_jti() -> Result<String, String> {
    let mut bytes = [0u8; 16];
    getrandom(&mut bytes).map_err(|e| format!("generate jti gagal: {e}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn issue_jwt(email: &str, nama: &str, role: &str, jti: &str, expires_at: DateTime<Utc>) -> Result<String, String> {
    let secret = jwt_secret()?;
    let claims = JwtClaims {
        sub: email.to_string(),
        nama: nama.to_string(),
        role: role.to_string(),
        jti: jti.to_string(),
        exp: expires_at.timestamp().max(0) as usize,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("encode JWT gagal: {e}"))
}

fn decode_jwt(token: &str, validate_exp: bool) -> Result<JwtClaims, String> {
    let secret = jwt_secret()?;
    let mut validation = Validation::default();
    validation.validate_exp = validate_exp;
    decode::<JwtClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map(|data| data.claims)
    .map_err(|e| format!("JWT tidak valid: {e}"))
}

fn claims_to_session(claims: &JwtClaims) -> AuthSession {
    AuthSession {
        nama: claims.nama.clone(),
        role: claims.role.clone(),
        expires_at: DateTime::from_timestamp(claims.exp as i64, 0).unwrap_or_else(Utc::now),
    }
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

    let email = user.email.clone();
    let nama = user.nama.unwrap_or_default();
    let role = user.role.unwrap_or_default();
    let expires_at = Utc::now() + Duration::seconds(jwt_expiry_secs());
    let jti = generate_jti()?;
    let token = issue_jwt(&email, &nama, &role, &jti, expires_at)?;

    let auth = AuthSession {
        nama: nama.clone(),
        role: role.clone(),
        expires_at,
    };
    store.write().await.insert(jti, auth.clone());

    Ok((token, auth))
}

pub async fn logout(store: &SessionStore, token: &str) -> bool {
    let Ok(claims) = decode_jwt(token, false) else {
        return false;
    };
    store.write().await.remove(&claims.jti).is_some()
}

pub async fn validate_session(store: &SessionStore, token: &str) -> Result<AuthSession, String> {
    let claims = decode_jwt(token, true)?;
    let guard = store.read().await;
    if !guard.contains_key(&claims.jti) {
        return Err("session tidak valid atau sudah logout".into());
    }
    Ok(claims_to_session(&claims))
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

/// Batasi RPC scrape Stockbit ke Senin–Jumat, jam 08:45–12:15 dan 13:25–16:15 (server lokal).
pub fn require_stockbit_scrape_hours() -> Result<(), tonic::Status> {
    let now = Local::now();
    match now.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => {
            return Err(tonic::Status::failed_precondition(
                "Diluar hari operasional Senin-Jumat (Sabtu/Minggu tidak scrape)",
            ));
        }
        _ => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 8 * 60 + 45;
    const MORNING_END: u32 = 12 * 60 + 16;
    const AFTERNOON_START: u32 = 13 * 60 + 25;
    const AFTERNOON_END: u32 = 16 * 60 + 16;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    if !in_morning && !in_afternoon {
        return Err(tonic::Status::failed_precondition(
            "Diluar jam 08:45-12:15 dan 13:25-16:15",
        ));
    }
    Ok(())
}
