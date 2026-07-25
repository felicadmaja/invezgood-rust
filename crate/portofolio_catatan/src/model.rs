//! Model Scylla untuk tabel `stockbit.portofolio_catatan`.
//! Lihat `portofolio_catatan.cql`.
//!
//! | Kolom CQL        | Tipe CQL | Rust   |
//! |------------------|----------|--------|
//! | emiten_name (PK) | text     | String |
//! | catatan          | text     | String |

use scylla::DeserializeRow;

use crate::PortofolioCatatanRow;

/// Baris tabel dasar `portofolio_catatan`.
/// PK: `(("emiten_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioCatatan {
    /// Partition key — kode emiten / ticker (4 huruf UPPERCASE).
    #[scylla(default_when_null)]
    pub emiten_name: String,
    /// Catatan bebas untuk emiten; kosong bila belum di-set.
    #[scylla(default_when_null)]
    pub catatan: String,
}

impl PortofolioCatatan {
    pub fn into_proto(self) -> PortofolioCatatanRow {
        PortofolioCatatanRow {
            emiten_name: self.emiten_name,
            catatan: self.catatan,
        }
    }
}
