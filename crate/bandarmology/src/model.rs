//! Model baris tabel `invezgood.bandarmology`.

use scylla::DeserializeRow;
use scylla::DeserializeValue;
use scylla::SerializeRow;
use scylla::SerializeValue;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "bandarmology";

/// UDT `bandarmology_entry` — ringkasan transaksi satu broker.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BandarmologyEntryDb {
    pub code: String,
    pub buy_freq: String,
    pub buy_volume: String,
    pub buy_value: String,
    pub sell_freq: String,
    pub sell_volume: String,
    pub sell_value: String,
    #[scylla(default_when_null)]
    pub buy_avg: Option<f64>,
    #[scylla(default_when_null)]
    pub sell_avg: Option<f64>,
    pub net_value: String,
    pub net_volume: String,
    pub net_freq: String,
    pub name: String,
}

/// Kolom `bandarmology` — list entri per broker.
pub type BandarmologyEntryListDb = Option<Vec<BandarmologyEntryDb>>;

/// Satu baris `invezgood.bandarmology`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct BandarmologyRow {
    pub code: String,
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    #[scylla(default_when_null)]
    pub bandarmology: BandarmologyEntryListDb,
    #[scylla(default_when_null)]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}
