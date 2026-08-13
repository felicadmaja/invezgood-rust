//! Model baris tabel `invezgood.msci`.

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "msci";

/// Satu baris `invezgood.msci`.
/// PK: `code` (partition key).
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct MsciRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub grade: Option<String>,
    #[scylla(default_when_null)]
    pub status: Option<String>,
    #[scylla(default_when_null)]
    pub updated_at: Option<DateTime<Utc>>,
}
