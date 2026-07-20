//! Model Scylla untuk tabel `stockbit.pending_order` + MV terkait.
//! Lihat `pending_order.cql`.
//!
//! ## Tabel dasar `pending_order`
//! PK: `(("order_id"))`
//!
//! | Kolom CQL              | Tipe CQL | Rust      |
//! |------------------------|----------|-----------|
//! | order_id (PK)          | text     | String    |
//! | emiten_name            | text     | String    |
//! | status                 | text     | String    |
//! | message                | text     | String    |
//! | side                   | text     | String    |
//! | time_open              | timestamp | Option\<DateTime\<Utc\>\> |
//! | lot_open               | double   | f64       |
//! | lot_done               | double   | f64       |
//! | price_order            | double   | f64       |
//! | amount_open            | double   | f64       |
//! | amount_match           | double   | f64       |
//! | amount_match_total     | double   | f64       |
//! | is_gtc                 | boolean  | bool      |
//! | updated_at             | timestamp | Option\<DateTime\<Utc\>\> |
//!
//! ## MV `pending_order_by_emiten_name`
//! PK: `(("emiten_name"), "order_id")` — `SELECT *` dari base table.
//!
//! ## MV `pending_order_by_status`
//! PK: `(("status"), "order_id")` — `SELECT *` dari base table.

use chrono::{DateTime, Utc};
use scylla::DeserializeRow;

use crate::PendingOrderRow;

/// Baris tabel dasar `pending_order` (juga hasil query MV `SELECT *`).
/// PK: `(("order_id"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PendingOrder {
    /// Partition key — ID order.
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
    /// Waktu terakhir baris di-upsert.
    pub updated_at: Option<DateTime<Utc>>,
}

impl PendingOrder {
    pub fn into_proto(self) -> PendingOrderRow {
        PendingOrderRow {
            order_id: self.order_id,
            emiten_name: self.emiten_name,
            status: self.status,
            message: self.message,
            side: self.side,
            time_open: self
                .time_open
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
            lot_open: self.lot_open,
            lot_done: self.lot_done,
            price_order: self.price_order,
            amount_open: self.amount_open,
            amount_match: self.amount_match,
            amount_match_total: self.amount_match_total,
            is_gtc: self.is_gtc,
            updated_at: self
                .updated_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}

/// Baris MV `pending_order_by_emiten_name` (lookup per emiten).
/// PK: `(("emiten_name"), "order_id")`.
///
/// Untuk baris lengkap dari MV (`SELECT *`), gunakan [`PendingOrder`].
#[derive(Debug, Clone, DeserializeRow)]
pub struct PendingOrderByEmitenName {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub order_id: String,
}

/// Baris MV `pending_order_by_status` (lookup per status).
/// PK: `(("status"), "order_id")`.
///
/// Untuk baris lengkap dari MV (`SELECT *`), gunakan [`PendingOrder`].
#[derive(Debug, Clone, DeserializeRow)]
pub struct PendingOrderByStatus {
    #[scylla(default_when_null)]
    pub status: String,
    #[scylla(default_when_null)]
    pub order_id: String,
}
