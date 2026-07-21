//! Model Scylla untuk tabel `stockbit.portofolio_bandarmology`.
//! Skema: `portofolio_bandarmology.cql`.
//!
//! ## Tabel dasar `portofolio_bandarmology`
//! PK: `(("emiten_name"), tahun_bulan_tanggal DESC)`
//!
//! | Kolom CQL            | Tipe CQL                    | Rust                      |
//! |----------------------|-----------------------------|---------------------------|
//! | emiten_name (PK)     | text                        | String                    |
//! | tahun_bulan_tanggal  | date (CK DESC)              | NaiveDate                 |
//! | bandarmology         | frozen\<bandarmology_day\>  | Option\<BandarmologyDay\> |
//!
//! UDT `bandarmology_day` (dan nested) sama dengan keyspace `stockbit.bandarmology`.

use chrono::NaiveDate;
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

/// UDT `bandarmology_top_stats`: volume, percent, rp_b, acc_dist.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct BandarmologyTopStats {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    #[scylla(default_when_null)]
    pub acc_dist: String,
}

/// UDT `bandarmology_broker_buy`: broker_code, buy_volume, buy_lot, buy_avg.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct BandarmologyBrokerBuy {
    #[scylla(default_when_null)]
    pub broker_code: String,
    #[scylla(default_when_null)]
    pub buy_volume: String,
    #[scylla(default_when_null)]
    pub buy_lot: String,
    pub buy_avg: i64,
}

/// UDT `bandarmology_broker_sell`: broker_code, sell_volume, sell_lot, sell_avg.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct BandarmologyBrokerSell {
    #[scylla(default_when_null)]
    pub broker_code: String,
    #[scylla(default_when_null)]
    pub sell_volume: String,
    #[scylla(default_when_null)]
    pub sell_lot: String,
    pub sell_avg: i64,
}

/// UDT `bandarmology_day` — snapshot Bandar Detector (Top 1/3/5, broker BY/SL, net summary).
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct BandarmologyDay {
    pub top_1: BandarmologyTopStats,
    pub top_3: BandarmologyTopStats,
    pub top_5: BandarmologyTopStats,
    pub average: BandarmologyTopStats,
    pub net_volume: i64,
    #[scylla(default_when_null)]
    pub net_value: String,
    pub average_rp: i64,
    pub broker_buy: Vec<BandarmologyBrokerBuy>,
    pub broker_sell: Vec<BandarmologyBrokerSell>,
}

/// Baris tabel dasar `portofolio_bandarmology`.
/// PK: `(("emiten_name"), tahun_bulan_tanggal DESC)`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioBandarmology {
    /// Partition key — kode emiten (contoh `BBCA`).
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Clustering key DESC — tanggal snapshot (contoh `2026-07-17`).
    pub tahun_bulan_tanggal: NaiveDate,
    /// Snapshot Bandar Detector untuk tanggal tersebut.
    pub bandarmology: Option<BandarmologyDay>,
}
