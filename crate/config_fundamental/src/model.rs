//! Model baris tabel `invezgood.config_fundamental`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "config_fundamental";

/// Satu baris `invezgood.config_fundamental`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct ConfigFundamentalRow {
    pub key: String,
    pub value: f64,
    #[scylla(default_when_null)]
    pub description: Option<String>,
}
