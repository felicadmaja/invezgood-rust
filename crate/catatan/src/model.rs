//! Model Scylla untuk tabel `stockbit.catatan`.
//! Skema: `catatan.cql` (hasil `create_catatan`).
//!
//! PK: `(("agg_tahun_bulan_tanggal_emiten_name"))`.

use chrono::NaiveDate;
use scylla::DeserializeRow;

/// Baris tabel dasar `catatan`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct Catatan {
    /// Partition key, contoh `2026-07-17_BBCA`.
    #[scylla(default_when_null)]
    pub agg_tahun_bulan_tanggal_emiten_name: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub catatan: String,
}

/// Membentuk partition key `concat(tahun_bulan_tanggal, '_', emiten_name)`.
pub fn agg_tahun_bulan_tanggal_emiten_name(
    tahun_bulan_tanggal: NaiveDate,
    emiten_name: &str,
) -> String {
    format!(
        "{}_{}",
        tahun_bulan_tanggal.format("%Y-%m-%d"),
        emiten_name.trim().to_uppercase()
    )
}
