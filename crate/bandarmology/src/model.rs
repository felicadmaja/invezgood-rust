use chrono::NaiveDate;
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};

use crate::{
    BandarmologyBrokerBuy as ProtoBrokerBuy, BandarmologyBrokerSell as ProtoBrokerSell,
    BandarmologyDay as ProtoDay, BandarmologyRow, BandarmologyTopStats as ProtoTopStats,
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

/// Baris tabel dasar `bandarmology`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct Bandarmology {
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_emiten_name: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub tahun_bulan: String,
    pub broker_summary: Option<BandarmologyDay>,
}

impl Bandarmology {
    pub fn into_proto(self) -> BandarmologyRow {
        BandarmologyRow {
            agg_tahun_bulan_emiten_name: self.agg_tahun_bulan_emiten_name,
            emiten_name: self.emiten_name,
            tahun_bulan: self.tahun_bulan,
            broker_summary: self.broker_summary.map(BandarmologyDay::into_proto),
        }
    }
}

/// Baris MV `bandarmology_by_emiten_name` (lookup per emiten).
#[derive(Debug, Clone, DeserializeRow)]
pub struct BandarmologyByEmitenName {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_emiten_name: String,
}

/// Format `tahun_bulan` dari tanggal lokal, contoh `2026-07`.
pub fn tahun_bulan_from_date(date: NaiveDate) -> String {
    date.format("%Y-%m").to_string()
}

/// Kunci partition: `concat(tahun_bulan, '_', emiten_name)` — contoh `2026-07_BBCA`.
pub fn agg_tahun_bulan_emiten_name(tahun_bulan: &str, emiten_name: &str) -> String {
    format!(
        "{}_{}",
        tahun_bulan.trim(),
        emiten_name.trim().to_ascii_uppercase()
    )
}

pub fn agg_tahun_bulan_emiten_name_from_date(date: NaiveDate, emiten_name: &str) -> String {
    agg_tahun_bulan_emiten_name(&tahun_bulan_from_date(date), emiten_name)
}
