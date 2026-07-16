use chrono::NaiveDate;

use crate::EmitenTrendingRow;

/// Baris tabel dasar `emiten_trending`.
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrending {
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub gainer_or_loser: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    pub price: f64,
    pub price_change: f64,
    #[scylla(default_when_null)]
    pub value: String,
    #[scylla(default_when_null)]
    pub volume: String,
}

impl EmitenTrending {
    pub fn into_proto(self) -> EmitenTrendingRow {
        EmitenTrendingRow {
            agg_tahun_bulan_tanggal_emiten_name: self.agg_tahun_bulan_tanggal_emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            gainer_or_loser: self.gainer_or_loser,
            emiten_name: self.emiten_name,
            price: self.price,
            price_change: self.price_change,
            value: self.value,
            volume: self.volume,
        }
    }
}

/// Baris MV `emiten_trending_by_emiten_name` (lookup per emiten).
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrendingByEmitenName {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
}

/// Baris MV `emiten_trending_by_tahun_bulan_tanggal` (lookup per tanggal).
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrendingByTahunBulanTanggal {
    pub tahun_bulan_tanggal: NaiveDate,
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
