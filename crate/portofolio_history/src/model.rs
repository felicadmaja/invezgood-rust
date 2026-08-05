//! Model baris tabel `invezgood.portofolio_history`.

use chrono::NaiveDate;
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

use crate::pb::{PortofolioHistoryItem as ProtoHistoryItem, PortofolioHistoryRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "portofolio_history";

/// UDT `portofolio_history_item`.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct PortofolioHistoryItem {
    #[scylla(default_when_null)]
    pub command: String,
    #[scylla(default_when_null)]
    pub symbol: String,
    #[scylla(default_when_null)]
    pub price: f64,
    #[scylla(default_when_null)]
    pub lot: f64,
    #[scylla(default_when_null)]
    pub amount: f64,
    #[scylla(default_when_null)]
    pub status: String,
}

impl PortofolioHistoryItem {
    pub fn into_proto(self) -> ProtoHistoryItem {
        ProtoHistoryItem {
            command: self.command,
            symbol: self.symbol,
            price: self.price,
            lot: self.lot,
            amount: self.amount,
            status: self.status,
        }
    }
}

/// Baris `invezgood.portofolio_history`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioHistory {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub history: Vec<PortofolioHistoryItem>,
}

impl PortofolioHistory {
    pub fn into_proto(self) -> PortofolioHistoryRow {
        PortofolioHistoryRow {
            emiten_name: self.emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            history: self
                .history
                .into_iter()
                .map(PortofolioHistoryItem::into_proto)
                .collect(),
        }
    }
}
