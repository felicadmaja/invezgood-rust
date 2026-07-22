//! Model Scylla untuk tabel `stockbit.portofolio_history`.
//! Skema: `portofolio_history.cql`.
//!
//! ## Tabel dasar `portofolio_history`
//! PK: `(("emiten_name"), tahun_bulan_tanggal DESC)`
//!
//! | Kolom CQL           | Tipe CQL | Rust |
//! |---------------------|----------|------|
//! | emiten_name (PK)    | text     | String |
//! | tahun_bulan_tanggal | date (CK DESC) | NaiveDate |
//! | history             | map\<timestamp, frozen\<portofolio_history_item\>\> | HashMap\<DateTime\<Utc\>, PortofolioHistoryItem\> |

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

use crate::{
    PortofolioHistoryItem as ProtoHistoryItem, PortofolioHistoryRow,
};

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

/// Baris tabel dasar `portofolio_history`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioHistory {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub history: HashMap<DateTime<Utc>, PortofolioHistoryItem>,
}

impl PortofolioHistory {
    pub fn into_proto(self) -> PortofolioHistoryRow {
        let history: HashMap<String, ProtoHistoryItem> = self
            .history
            .into_iter()
            .map(|(ts, item)| (ts.to_rfc3339(), item.into_proto()))
            .collect();
        PortofolioHistoryRow {
            emiten_name: self.emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            history,
        }
    }
}
