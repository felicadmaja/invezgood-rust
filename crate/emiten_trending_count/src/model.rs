//! Model Scylla untuk tabel `stockbit.emiten_trending_count_by_name`.
//! Skema: `emiten_trending_count.cql`.
//!
//! ## Tabel dasar `emiten_trending_count_by_name`
//! PK: `(("emiten_name"))`
//!
//! | Kolom CQL                 | Tipe CQL  | Rust                      |
//! |---------------------------|-----------|---------------------------|
//! | emiten_name (PK)          | text      | String                    |
//! | appearance_count          | bigint    | i64                       |
//! | last_tahun_bulan_tanggal  | date      | NaiveDate                 |
//! | updated_at                | timestamp | Option\<DateTime\<Utc\>\> |

use chrono::{DateTime, NaiveDate, Utc};
use scylla::DeserializeRow;

use crate::EmitenTrendingCountByNameRow;

/// Baris tabel dasar `emiten_trending_count_by_name`.
/// PK: `(("emiten_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct EmitenTrendingCountByName {
    /// Partition key — kode emiten (contoh `BBCA`).
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Jumlah kemunculan emiten di tabel `emiten_trending` (hari unik).
    #[scylla(default_when_null)]
    pub appearance_count: i64,
    /// Tanggal trending terakhir yang dihitung untuk emiten ini.
    pub last_tahun_bulan_tanggal: Option<NaiveDate>,
    /// Waktu terakhir baris count diperbarui.
    pub updated_at: Option<DateTime<Utc>>,
}

impl EmitenTrendingCountByName {
    pub fn into_proto(self) -> EmitenTrendingCountByNameRow {
        EmitenTrendingCountByNameRow {
            emiten_name: self.emiten_name,
            appearance_count: self.appearance_count,
            last_tahun_bulan_tanggal: self
                .last_tahun_bulan_tanggal
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
            updated_at: self
                .updated_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}
