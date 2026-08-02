//! Model baris tabel `invezgood.broker`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "broker";

/// Satu baris `invezgood.broker`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BrokerRow {
    pub broker_code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub tipe: Option<String>,
    #[scylla(default_when_null)]
    pub asosiasi: Option<String>,
    #[scylla(default_when_null)]
    pub catatan: Option<String>,
    #[scylla(default_when_null)]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}
