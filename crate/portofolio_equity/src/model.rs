//! Model Scylla untuk tabel `stockbit.portofolio_equity`.
//! Lihat `portofolio_equity.cql`.
//!
//! ## Tabel dasar `portofolio_equity`
//! PK: `(("nama"))`
//!
//! | Kolom CQL   | Tipe CQL | Rust   |
//! |-------------|----------|--------|
//! | nama (PK)   | text     | String |
//! | value       | double   | f64    |

use scylla::DeserializeRow;

use crate::PortofolioEquityRow;

/// Baris tabel dasar `portofolio_equity`.
/// PK: `(("nama"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioEquity {
    /// Partition key — nama metrik equity (contoh `Trading Balance`).
    #[scylla(default_when_null)]
    pub nama: String,
    #[scylla(default_when_null)]
    pub value: f64,
}

impl PortofolioEquity {
    pub fn into_proto(self) -> PortofolioEquityRow {
        PortofolioEquityRow {
            nama: self.nama,
            value: self.value,
        }
    }
}
