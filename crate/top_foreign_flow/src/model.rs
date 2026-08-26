//! Model baris tabel `invezgood.top_foreign_flow`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "top_foreign_flow";
pub const MV_BY_TAHUN_BULAN_TANGGAL: &str = "top_foreign_flow_by_tahun_bulan_tanggal";
pub const MV_BY_CODE: &str = "top_foreign_flow_by_code";

/// Baris PK-only MV `top_foreign_flow_by_tahun_bulan_tanggal` / `top_foreign_flow_by_code`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct TopForeignFlowPkRow {
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    pub value: i64,
    pub code: String,
}

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
