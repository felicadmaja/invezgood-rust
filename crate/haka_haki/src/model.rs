//! Model baris tabel `invezgood.haka_haki`.

use chrono::NaiveDate;
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "haka_haki";
pub const MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL: &str = "haka_haki_by_agg_code_tahun_bulan_tanggal";

/// `{code}_{YYYY-MM-DD}` — contoh `BBCA_2026-08-07`.
pub fn agg_code_tahun_bulan_tanggal(code: &str, trade_date: NaiveDate) -> String {
    format!("{}_{}", code.trim().to_ascii_uppercase(), trade_date.format("%Y-%m-%d"))
}

use crate::pb::HakaHakiPoint;

/// Satu baris `invezgood.haka_haki`.
/// PK: `(code, tahun_bulan_tanggal DESC, jam_menit DESC)`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct HakaHakiRow {
    #[scylla(default_when_null)]
    pub code: String,
    pub tahun_bulan_tanggal: NaiveDate,
    #[scylla(default_when_null)]
    pub jam_menit: String,
    #[scylla(default_when_null)]
    pub agg_code_tahun_bulan_tanggal: String,
    #[scylla(default_when_null)]
    pub volume: i32,
    #[scylla(default_when_null)]
    pub buy: i32,
    #[scylla(default_when_null)]
    pub sell: i32,
}

impl HakaHakiRow {
    pub fn into_proto(self) -> HakaHakiPoint {
        HakaHakiPoint {
            time: self.jam_menit,
            value: i64::from(self.volume),
            buy: i64::from(self.buy),
            sell: i64::from(self.sell),
        }
    }
}
