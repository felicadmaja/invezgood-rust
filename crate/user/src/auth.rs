use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::model::UserRow;

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub nama: String,
    pub role: String,
}

pub type SessionStore = Arc<RwLock<HashMap<String, AuthSession>>>;

pub fn new_session_store() -> SessionStore {
    Arc::new(RwLock::new(HashMap::new()))
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
    };

    let token = Uuid::new_v4().to_string();
    store.write().await.insert(token.clone(), auth.clone());

    Ok((token, auth))
}

pub async fn logout(store: &SessionStore, token: &str) -> bool {
    store.write().await.remove(token).is_some()
}
