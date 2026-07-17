//! Model Scylla untuk tabel `stockbit.emiten_trending` + MV terkait.
//! Skema: `emiten_trending.cql` (hasil `create_emiten_trending`).
//!
//! ## Tabel dasar `emiten_trending`
//! PK: `(("agg_tahun_bulan_tanggal_emiten_name"))`
//!
//! | Kolom CQL                               | Tipe CQL  | Rust                         |
//! |-----------------------------------------|-----------|------------------------------|
//! | agg_tahun_bulan_tanggal_emiten_name (PK)| text      | String                       |
//! | tahun_bulan_tanggal                     | date      | NaiveDate                    |
//! | gainer_or_loser                         | text      | String                       |
//! | emiten_name                             | text      | String                       |
//! | emiten_icon                             | text      | String                       |
//! | price                                   | double    | f64                          |
//! | price_change                            | double    | f64                          |
//! | value                                   | text      | String                       |
//! | volume                                  | text      | String                       |
//! | freq                                    | text      | String                       |
//! | updated_at                              | timestamp | Option\<DateTime\<Utc\>\>    |
//!
//! ## MV `emiten_trending_by_emiten_name`
//! PK: `(("emiten_name"), "agg_tahun_bulan_tanggal_emiten_name")`
//!
//! ## MV `emiten_trending_by_tahun_bulan_tanggal`
//! PK: `(("tahun_bulan_tanggal"), "agg_tahun_bulan_tanggal_emiten_name")`

use chrono::{DateTime, NaiveDate, Utc};

use crate::EmitenTrendingRow;

/// Baris tabel dasar `emiten_trending`.
/// PK: `(("agg_tahun_bulan_tanggal_emiten_name"))`.
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrending {
    /// Partition key — `concat(tahun_bulan_tanggal, '_', emiten_name)`, contoh `2026-07-16_BBCA`.
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub gainer_or_loser: String,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Path object GCS modul `stoksaham` (hasil upload icon Movers).
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    #[scylla(default_when_null)]
    pub price: f64,
    #[scylla(default_when_null)]
    pub price_change: f64,
    #[scylla(default_when_null)]
    pub value: String,
    #[scylla(default_when_null)]
    pub volume: String,
    /// Frekuensi transaksi dari kolom Freq tabel Movers Stockbit.
    #[scylla(default_when_null)]
    pub freq: String,
    /// Waktu terakhir baris di-upsert.
    pub updated_at: Option<DateTime<Utc>>,
}

impl EmitenTrending {
    pub fn into_proto(self) -> EmitenTrendingRow {
        EmitenTrendingRow {
            agg_tahun_bulan_tanggal_emiten_name: self.agg_tahun_bulan_tanggal_emiten_name,
            tahun_bulan_tanggal: self.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            gainer_or_loser: self.gainer_or_loser,
            emiten_name: self.emiten_name,
            emiten_icon: self.emiten_icon,
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

/// Baris MV `emiten_trending_by_emiten_name` (lookup per emiten).
/// PK: `(("emiten_name"), "agg_tahun_bulan_tanggal_emiten_name")`.
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct EmitenTrendingByEmitenName {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
}

/// Baris MV `emiten_trending_by_tahun_bulan_tanggal` (lookup per tanggal).
/// PK: `(("tahun_bulan_tanggal"), "agg_tahun_bulan_tanggal_emiten_name")`.
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
