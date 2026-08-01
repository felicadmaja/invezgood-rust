//! Model baris tabel `invezgood.user`.

use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "user";

/// Satu baris `invezgood.user`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct UserRow {
    pub email: String,
    #[scylla(default_when_null)]
    pub nama: Option<String>,
    #[scylla(default_when_null)]
    pub password: Option<String>,
    #[scylla(default_when_null)]
    pub role: Option<String>,
}
