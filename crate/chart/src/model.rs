//! Model baris tabel `invezgood.chart`.
//!
//! PK: `(code, date)` — clustering `date DESC`.

use chrono::NaiveDate;
use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "chart";

/// Satu baris OHLCV `invezgood.chart`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct ChartRow {
    /// Partition key — kode emiten (4 huruf).
    #[scylla(default_when_null)]
    pub code: String,
    /// Clustering key — tanggal bar (DESC).
    pub date: NaiveDate,
    #[scylla(default_when_null)]
    pub open: i32,
    #[scylla(default_when_null)]
    pub high: i32,
    #[scylla(default_when_null)]
    pub low: i32,
    #[scylla(default_when_null)]
    pub close: i32,
    #[scylla(default_when_null)]
    pub volume: i32,
}
