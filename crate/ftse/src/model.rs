//! Model baris tabel `invezgood.ftse`.

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "ftse";

/// Satu baris `invezgood.ftse`.
/// PK: `code` (partition key). Kolom selaras `invezgood.msci`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct FtseRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub grade: Option<String>,
    #[scylla(default_when_null)]
    pub status: Option<String>,
    #[scylla(default_when_null)]
    pub updated_at: Option<DateTime<Utc>>,
}
