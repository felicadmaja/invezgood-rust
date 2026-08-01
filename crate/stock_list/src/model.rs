//! Model baris tabel `invezgood.stock_list`.

use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "stock_list";

/// Satu baris `invezgood.stock_list`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct StockListRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
}
