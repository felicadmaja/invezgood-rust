//! Model baris tabel `invezgood.top_foreign_flow`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "top_foreign_flow";

/// Satu baris `invezgood.top_foreign_flow`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct TopForeignFlowRow {
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    pub value: i64,
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub price: Option<i32>,
    #[scylla(default_when_null)]
    pub change: Option<f64>,
    #[scylla(default_when_null)]
    pub volume: Option<i64>,
    #[scylla(default_when_null)]
    pub accum_or_dist: Option<String>,
}
