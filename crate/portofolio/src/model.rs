//! Model Scylla untuk tabel `stockbit.portofolio`.
//! Skema: `portofolio.cql` (hasil `create_portofolio`).
//!
//! ## Tabel dasar `portofolio`
//! PK: `(("emiten_name"))`
//!
//! | Kolom CQL      | Tipe CQL | Rust   |
//! |----------------|----------|--------|
//! | emiten_name (PK)| text    | String |
//! | long_name      | text     | String |
//! | emiten_icon    | text     | String |
//! | balance_lot    | bigint   | i64    |
//! | available_lot  | bigint   | i64    |
//! | average_price  | double   | f64    |
//! | current_price  | double   | f64    |
//! | invested       | double   | f64    |
//! | market_value   | double   | f64    |
//! | potential_p_l  | double   | f64    |
//! | percentage     | double   | f64    |
//! | history        | map\<timestamp, frozen\<portofolio_history_item\>\> | HashMap\<DateTime\<Utc\>, PortofolioHistoryItem\> |

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

use crate::{PortofolioHistoryItem as ProtoHistoryItem, PortofolioRow};

/// UDT `portofolio_history_item` — satu entri matched order di `history`.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct PortofolioHistoryItem {
    #[scylla(default_when_null)]
    pub order_id: String,
    #[scylla(default_when_null)]
    pub message: String,
    #[scylla(default_when_null)]
    pub symbol: String,
    #[scylla(default_when_null)]
    pub side: String,
    #[scylla(default_when_null)]
    pub lot_done: i32,
    #[scylla(default_when_null)]
    pub price_average: f64,
    #[scylla(default_when_null)]
    pub amount_matched: f64,
}

impl PortofolioHistoryItem {
    pub fn into_proto(self) -> ProtoHistoryItem {
        ProtoHistoryItem {
            order_id: self.order_id,
            message: self.message,
            symbol: self.symbol,
            side: self.side,
            lot_done: self.lot_done,
            price_average: self.price_average,
            amount_matched: self.amount_matched,
        }
    }
}

/// Baris tabel dasar `portofolio`.
/// PK: `(("emiten_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct Portofolio {
    /// Partition key — kode emiten (contoh `BBCA`).
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Nama perusahaan panjang (contoh `Bank Central Asia Tbk`).
    #[scylla(default_when_null)]
    pub long_name: String,
    /// Path object GCS modul `stoksaham` (hasil upload icon emiten).
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    #[scylla(default_when_null)]
    pub balance_lot: i64,
    #[scylla(default_when_null)]
    pub available_lot: i64,
    #[scylla(default_when_null)]
    pub average_price: f64,
    #[scylla(default_when_null)]
    pub current_price: f64,
    #[scylla(default_when_null)]
    pub invested: f64,
    #[scylla(default_when_null)]
    pub market_value: f64,
    #[scylla(default_when_null)]
    pub potential_p_l: f64,
    #[scylla(default_when_null)]
    pub percentage: f64,
    /// Riwayat matched order: key = waktu order, value = detail UDT.
    #[scylla(default_when_null)]
    pub history: HashMap<DateTime<Utc>, PortofolioHistoryItem>,
}

impl Portofolio {
    pub fn into_proto(self) -> PortofolioRow {
        let history: HashMap<String, ProtoHistoryItem> = self
            .history
            .into_iter()
            .map(|(ts, item)| (ts.to_rfc3339(), item.into_proto()))
            .collect();
        PortofolioRow {
            emiten_name: self.emiten_name,
            long_name: self.long_name,
            emiten_icon: self.emiten_icon,
            balance_lot: self.balance_lot,
            available_lot: self.available_lot,
            average_price: self.average_price,
            current_price: self.current_price,
            invested: self.invested,
            market_value: self.market_value,
            potential_p_l: self.potential_p_l,
            percentage: self.percentage,
            history,
        }
    }
}
