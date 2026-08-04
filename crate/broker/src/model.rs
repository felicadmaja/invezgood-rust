//! Model baris tabel `invezgood.broker` + `invezgood.broker_stalker`.

use std::collections::HashMap;

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "broker";
pub const TABLE_BROKER_STALKER: &str = "broker_stalker";

/// Satu baris `invezgood.broker`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BrokerRow {
    pub broker_code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub tipe: Option<i8>,
    #[scylla(default_when_null)]
    pub asosiasi: Option<String>,
    #[scylla(default_when_null)]
    pub catatan: Option<String>,
    #[scylla(default_when_null)]
    pub is_huge: Option<bool>,
    #[scylla(default_when_null)]
    pub is_top: Option<bool>,
    #[scylla(default_when_null)]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Satu baris `invezgood.broker_stalker`.
/// PK: ((broker_code), tahun_bulan DESC).
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BrokerStalkerRow {
    pub broker_code: String,
    pub tahun_bulan: String,
    #[scylla(default_when_null)]
    pub summary: Option<HashMap<String, String>>,
    #[scylla(default_when_null)]
    pub list: Option<Vec<HashMap<String, String>>>,
}
