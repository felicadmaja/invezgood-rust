//! Model Scylla untuk tabel `stockbit.wyckoff_glossary`.
//! Lihat `wyckoff_glossary.cql`.
//!
//! | Kolom CQL     | Tipe CQL | Rust   |
//! |---------------|----------|--------|
//! | name (PK)     | text     | String |
//! | description   | text     | String |

use scylla::DeserializeRow;

/// Baris tabel dasar `wyckoff_glossary`.
/// PK: `(("name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct WyckoffGlossary {
    /// Partition key — istilah / nama entri glosarium.
    #[scylla(default_when_null)]
    pub name: String,
    /// Penjelasan istilah; kosong bila belum di-set.
    #[scylla(default_when_null)]
    pub description: String,
}
