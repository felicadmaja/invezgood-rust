//! Model baris tabel `invezgood.pending_order`.

use chrono::{DateTime, NaiveDate, Utc};
use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "pending_order";
pub const MV_BY_EMITEN: &str = "pending_order_by_emiten_name";
pub const MV_BY_TAHUN_BULAN_TANGGAL: &str = "pending_order_by_tahun_bulan_tanggal";
pub const MV_BY_TAHUN_BULAN: &str = "pending_order_by_tahun_bulan";

/// `YYYY-MM` dari tanggal order.
pub fn tahun_bulan_from_date(date: NaiveDate) -> String {
    date.format("%Y-%m").to_string()
}

/// Satu baris `invezgood.pending_order` (juga hasil query MV).
/// PK: `((order_id), tahun_bulan_tanggal DESC)`.
/// `tahun_bulan_tanggal` = tanggal invoke scrape/API (bukan dari `time_open`).
#[derive(Debug, Clone, DeserializeRow)]
pub struct PendingOrderRow {
    #[scylla(default_when_null)]
    pub order_id: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub tahun_bulan: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub status: String,
    #[scylla(default_when_null)]
    pub message: String,
    #[scylla(default_when_null)]
    pub side: String,
    pub time_open: Option<DateTime<Utc>>,
    #[scylla(default_when_null)]
    pub lot_open: f64,
    #[scylla(default_when_null)]
    pub lot_done: f64,
    #[scylla(default_when_null)]
    pub price_order: f64,
    #[scylla(default_when_null)]
    pub amount_open: f64,
    #[scylla(default_when_null)]
    pub amount_match: f64,
    #[scylla(default_when_null)]
    pub amount_match_total: f64,
    #[scylla(default_when_null)]
    pub is_gtc: bool,
    pub updated_at: Option<DateTime<Utc>>,
}
