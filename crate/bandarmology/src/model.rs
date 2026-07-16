use chrono::NaiveDate;

/// UDT `bandarmology_top_stats`: volume, percent, rp_b, acc_dist.
#[derive(Debug, Clone, scylla::DeserializeRow, scylla::SerializeRow)]
pub struct BandarmologyTopStats {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    #[scylla(default_when_null)]
    pub acc_dist: String,
}

/// UDT `bandarmology_broker_buy`: broker_code, buy_volume, buy_lot, buy_avg.
#[derive(Debug, Clone, scylla::DeserializeRow, scylla::SerializeRow)]
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
#[derive(Debug, Clone, scylla::DeserializeRow, scylla::SerializeRow)]
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
#[derive(Debug, Clone, scylla::DeserializeRow, scylla::SerializeRow)]
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

/// Baris tabel dasar `bandarmology`.
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct Bandarmology {
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    pub d_1: Option<BandarmologyDay>,
    pub d_2: Option<BandarmologyDay>,
    pub d_7: Option<BandarmologyDay>,
    #[scylla(name = "M_1")]
    pub m_1: Option<BandarmologyDay>,
    #[scylla(name = "M_3")]
    pub m_3: Option<BandarmologyDay>,
    #[scylla(name = "M_6")]
    pub m_6: Option<BandarmologyDay>,
    #[scylla(name = "M_12")]
    pub m_12: Option<BandarmologyDay>,
}

/// Baris MV `bandarmology_by_emiten_name` (lookup per emiten).
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct BandarmologyByEmitenName {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
}

/// Kunci partition: `concat(tahun_bulan_tanggal, '_', emiten_name)` — contoh `2026-07-16_BBCA`.
pub fn agg_tahun_bulan_tanggal_emiten_name(
    tahun_bulan_tanggal: NaiveDate,
    emiten_name: &str,
) -> String {
    format!("{}_{}", tahun_bulan_tanggal.format("%Y-%m-%d"), emiten_name)
}
