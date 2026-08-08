//! Model baris tabel `invezgood.haka_haki`.

use chrono::NaiveDate;
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "haka_haki";

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
    pub volume: i32,
    #[scylla(default_when_null)]
    pub buy: i32,
    #[scylla(default_when_null)]
    pub sell: i32,
}
