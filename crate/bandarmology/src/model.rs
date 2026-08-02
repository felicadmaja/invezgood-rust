//! Model baris tabel `stockbit.bandarmology*`.

use scylla::DeserializeRow;
use scylla::DeserializeValue;
use scylla::SerializeRow;
use scylla::SerializeValue;

pub const KEYSPACE: &str = "stockbit";
pub const TABLE: &str = "bandarmology";
pub const TABLE_HARIAN: &str = "bandarmology_harian";
pub const TABLE_PORTOFOLIO: &str = "portofolio_bandarmology";

/// UDT `bandarmology_top_stats`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BandarmologyTopStatsDb {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    pub acc_dist: String,
}

/// UDT `bandarmology_broker_buy`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BandarmologyBrokerBuyDb {
    pub broker_code: String,
    pub buy_volume: String,
    pub buy_lot: String,
    pub buy_avg: i64,
}

/// UDT `bandarmology_broker_sell`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BandarmologyBrokerSellDb {
    pub broker_code: String,
    pub sell_volume: String,
    pub sell_lot: String,
    pub sell_avg: i64,
}

/// UDT `bandarmology_day`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BandarmologyDayDb {
    pub top_1: BandarmologyTopStatsDb,
    pub top_3: BandarmologyTopStatsDb,
    pub top_5: BandarmologyTopStatsDb,
    pub average: BandarmologyTopStatsDb,
    pub net_volume: i64,
    pub net_value: String,
    pub average_rp: i64,
    #[scylla(default_when_null)]
    pub broker_buy: Option<Vec<BandarmologyBrokerBuyDb>>,
    #[scylla(default_when_null)]
    pub broker_sell: Option<Vec<BandarmologyBrokerSellDb>>,
}

/// Satu baris `stockbit.bandarmology`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BandarmologyRow {
    pub agg_tahun_bulan_emiten_name: String,
    #[scylla(default_when_null)]
    pub emiten_name: Option<String>,
    #[scylla(default_when_null)]
    pub tahun_bulan: Option<String>,
    #[scylla(default_when_null)]
    pub broker_summary: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub broker_summary_current_w1: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub broker_summary_current_w2: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub broker_summary_current_w3: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub broker_summary_current_w4: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Satu baris `stockbit.bandarmology_harian`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BandarmologyHarianRow {
    pub emiten_name: String,
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    #[scylla(default_when_null)]
    pub broker_summary_harian: Option<BandarmologyDayDb>,
    #[scylla(default_when_null)]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Satu baris `stockbit.portofolio_bandarmology`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct PortofolioBandarmologyRow {
    pub emiten_name: String,
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    #[scylla(default_when_null)]
    pub bandarmology: Option<BandarmologyDayDb>,
}

pub fn agg_key(tahun_bulan: &str, code: &str) -> String {
    format!("{tahun_bulan}_{code}")
}
