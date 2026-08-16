//! Model baris tabel `invezgood.hari_libur`.

use chrono::{DateTime, NaiveDate, Utc};
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "hari_libur";
/// MV `invezgood.hari_libur_by_tahun` — PK (`tahun`, `date`), untuk ambil satu tahun.
pub const VIEW_BY_TAHUN: &str = "hari_libur_by_tahun";

/// Satu baris `invezgood.hari_libur`.
/// PK: `date` (partition key).
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct HariLiburRow {
    pub date: NaiveDate,
    /// Format `YYYY`, diturunkan dari `date`; partition key MV `hari_libur_by_tahun`.
    #[scylla(default_when_null)]
    pub tahun: Option<String>,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub is_civic: Option<bool>,
    #[scylla(default_when_null)]
    pub is_religious: Option<bool>,
    #[scylla(default_when_null)]
    pub is_cuti_bersama: Option<bool>,
    #[scylla(default_when_null)]
    pub updated_at: Option<DateTime<Utc>>,
}
