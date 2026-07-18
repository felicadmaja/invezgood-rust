//! Model Scylla untuk tabel `stockbit.broker`.
//! Lihat `broker.cql`.
//!
//! | Kolom CQL        | Tipe CQL | Rust |
//! |------------------|----------|------|
//! | broker_code (PK) | text     | String |
//! | name             | text     | String |
//! | tipe             | text     | String |
//! | asosiasi         | text     | String |
//! | catatan          | text     | String |
//!
//! Lookup: `WHERE broker_code = ?` (contoh kode broker IDX).
//! Tidak ada MV / secondary index — akses lewat PK `(("broker_code"))`.

use scylla::DeserializeRow;

use crate::BrokerRow;

/// Baris tabel dasar `broker`.
/// PK: `(("broker_code"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct Broker {
    /// Partition key — kode broker IDX (contoh `XL`, `YP`).
    #[scylla(default_when_null)]
    pub broker_code: String,
    #[scylla(default_when_null)]
    pub name: String,
    #[scylla(default_when_null)]
    pub tipe: String,
    #[scylla(default_when_null)]
    pub asosiasi: String,
    #[scylla(default_when_null)]
    pub catatan: String,
}

impl Broker {
    pub fn from_proto(row: BrokerRow) -> Self {
        Self {
            broker_code: row.broker_code.trim().to_ascii_uppercase(),
            name: row.name,
            tipe: row.tipe,
            asosiasi: row.asosiasi,
            catatan: row.catatan,
        }
    }

    pub fn into_proto(self) -> BrokerRow {
        BrokerRow {
            broker_code: self.broker_code,
            name: self.name,
            tipe: self.tipe,
            asosiasi: self.asosiasi,
            catatan: self.catatan,
        }
    }
}
