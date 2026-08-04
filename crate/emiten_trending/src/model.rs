//! Model Scylla `invezgood.emiten_trending` + MV terkait.

use chrono::{DateTime, NaiveDate, Utc};

use crate::pb::EmitenTrendingRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "emiten_trending";
pub const MV_BY_DATE: &str = "emiten_trending_by_tahun_bulan_tanggal";
pub const MV_BY_EMITEN: &str = "emiten_trending_by_emiten_name";

/// Baris tabel dasar / MV full-column `emiten_trending_by_tahun_bulan_tanggal`.
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrending {
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub gainer_or_loser: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub long_name: String,
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    pub sector: Option<i8>,
    #[scylla(default_when_null)]
    pub price: f64,
    #[scylla(default_when_null)]
    pub price_change: f64,
    #[scylla(default_when_null)]
    pub value: String,
    #[scylla(default_when_null)]
    pub volume: String,
    #[scylla(default_when_null)]
    pub freq: String,
    pub updated_at: Option<DateTime<Utc>>,
}

impl EmitenTrending {
    pub fn into_proto(self) -> EmitenTrendingRow {
        EmitenTrendingRow {
            agg_tahun_bulan_tanggal_emiten_name: self.agg_tahun_bulan_tanggal_emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            gainer_or_loser: self.gainer_or_loser,
            emiten_name: self.emiten_name,
            long_name: self.long_name,
            emiten_icon: self.emiten_icon,
            sector: i32::from(self.sector.unwrap_or(0).max(0)),
            price: self.price,
            price_change: self.price_change,
            value: self.value,
            volume: self.volume,
            freq: self.freq,
            updated_at: self
                .updated_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}

/// Kunci partition: `YYYY-MM-DD_EMITEN`.
pub fn agg_tahun_bulan_tanggal_emiten_name(
    tahun_bulan_tanggal: NaiveDate,
    emiten_name: &str,
) -> String {
    format!("{}_{}", tahun_bulan_tanggal.format("%Y-%m-%d"), emiten_name)
}
