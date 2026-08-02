//! Model baris tabel `invezgood.pending_order`.

use chrono::{DateTime, Utc};
use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "pending_order";
pub const MV_BY_EMITEN: &str = "pending_order_by_emiten_name";

/// Satu baris `invezgood.pending_order` (juga hasil query MV).
#[derive(Debug, Clone, DeserializeRow)]
pub struct PendingOrderRow {
    #[scylla(default_when_null)]
    pub order_id: String,
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
