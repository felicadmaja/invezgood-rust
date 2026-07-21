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

use crate::{
    BandarmologyBrokerBuy as ProtoBrokerBuy, BandarmologyBrokerSell as ProtoBrokerSell,
    BandarmologyDay as ProtoDay, BandarmologyTopStats as ProtoTopStats,
    PortofolioBandarmologyRow,
};

/// UDT `bandarmology_top_stats`: volume, percent, rp_b, acc_dist.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct BandarmologyTopStats {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    #[scylla(default_when_null)]
    pub acc_dist: String,
}

impl BandarmologyTopStats {
    pub fn into_proto(self) -> ProtoTopStats {
        ProtoTopStats {
            volume: self.volume,
            percent: self.percent,
            rp_b: self.rp_b,
            acc_dist: self.acc_dist,
        }
    }
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

impl BandarmologyBrokerBuy {
    pub fn into_proto(self) -> ProtoBrokerBuy {
        ProtoBrokerBuy {
            broker_code: self.broker_code,
            buy_volume: self.buy_volume,
            buy_lot: self.buy_lot,
            buy_avg: self.buy_avg,
        }
    }
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

impl BandarmologyBrokerSell {
    pub fn into_proto(self) -> ProtoBrokerSell {
        ProtoBrokerSell {
            broker_code: self.broker_code,
            sell_volume: self.sell_volume,
            sell_lot: self.sell_lot,
            sell_avg: self.sell_avg,
        }
    }
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

impl BandarmologyDay {
    pub fn into_proto(self) -> ProtoDay {
        ProtoDay {
            top_1: Some(self.top_1.into_proto()),
            top_3: Some(self.top_3.into_proto()),
            top_5: Some(self.top_5.into_proto()),
            average: Some(self.average.into_proto()),
            net_volume: self.net_volume,
            net_value: self.net_value,
            average_rp: self.average_rp,
            broker_buy: self
                .broker_buy
                .into_iter()
                .map(BandarmologyBrokerBuy::into_proto)
                .collect(),
            broker_sell: self
                .broker_sell
                .into_iter()
                .map(BandarmologyBrokerSell::into_proto)
                .collect(),
        }
    }
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

impl PortofolioBandarmology {
    pub fn into_proto(self) -> PortofolioBandarmologyRow {
        PortofolioBandarmologyRow {
            emiten_name: self.emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            bandarmology: self.bandarmology.map(BandarmologyDay::into_proto),
        }
    }
}
