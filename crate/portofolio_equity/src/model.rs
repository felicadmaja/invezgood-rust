//! Model baris tabel `invezgood.portofolio_equity`.

use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "portofolio_equity";

/// Satu baris `invezgood.portofolio_equity` — PK `nama`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioEquity {
    #[scylla(default_when_null)]
    pub nama: String,
    #[scylla(default_when_null)]
    pub value: f64,
}

impl PortofolioEquity {
    pub fn into_proto(self) -> crate::pb::PortofolioEquityRow {
        crate::pb::PortofolioEquityRow {
            nama: self.nama,
            value: self.value,
        }
    }
}
