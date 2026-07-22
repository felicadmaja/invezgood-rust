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
//! | history             | list\<frozen\<portofolio_history_item\>\> | Vec\<PortofolioHistoryItem\> |

use chrono::NaiveDate;
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

use crate::{
    PortofolioHistoryItem as ProtoHistoryItem, PortofolioHistoryRow,
};

/// UDT `portofolio_history_item` — satu entri matched order di `history`.
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

/// Baris tabel dasar `portofolio_history`.
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
