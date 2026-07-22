//! Model Scylla untuk tabel `stockbit.portofolio`.
//! Skema: `portofolio.cql`.
//!
//! ## Tabel dasar `portofolio`
//! PK: `(("emiten_name"))`
//!
//! | Kolom CQL       | Tipe CQL | Rust   |
//! |-----------------|----------|--------|
//! | emiten_name (PK)| text     | String |
//! | long_name       | text     | String |
//! | emiten_icon     | text     | String |
//! | balance_lot     | bigint   | i64    |
//! | available_lot   | bigint   | i64    |
//! | average_price   | double   | f64    |
//! | current_price   | double   | f64    |
//! | invested        | double   | f64    |
//! | market_value    | double   | f64    |
//! | potential_p_l   | double   | f64    |
//! | percentage      | double   | f64    |
//!
//! Riwayat order ada di tabel terpisah `portofolio_history` (bukan kolom di sini).

use scylla::DeserializeRow;

use crate::PortofolioRow;

/// Baris tabel dasar `portofolio`.
/// PK: `(("emiten_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct Portofolio {
    /// Partition key — kode emiten (contoh `BBCA`).
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Nama perusahaan panjang (contoh `Bank Central Asia Tbk`).
    #[scylla(default_when_null)]
    pub long_name: String,
    /// Path object GCS modul `stoksaham` (hasil upload icon emiten).
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    #[scylla(default_when_null)]
    pub balance_lot: i64,
    #[scylla(default_when_null)]
    pub available_lot: i64,
    #[scylla(default_when_null)]
    pub average_price: f64,
    #[scylla(default_when_null)]
    pub current_price: f64,
    #[scylla(default_when_null)]
    pub invested: f64,
    #[scylla(default_when_null)]
    pub market_value: f64,
    #[scylla(default_when_null)]
    pub potential_p_l: f64,
    #[scylla(default_when_null)]
    pub percentage: f64,
}

impl Portofolio {
    pub fn into_proto(self) -> PortofolioRow {
        PortofolioRow {
            emiten_name: self.emiten_name,
            long_name: self.long_name,
            emiten_icon: self.emiten_icon,
            balance_lot: self.balance_lot,
            available_lot: self.available_lot,
            average_price: self.average_price,
            current_price: self.current_price,
            invested: self.invested,
            market_value: self.market_value,
            potential_p_l: self.potential_p_l,
            percentage: self.percentage,
        }
    }
}
