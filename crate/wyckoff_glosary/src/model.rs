//! Model Scylla untuk tabel `stockbit.wyckoff_glossary`.
//! Lihat `wyckoff_glossary.cql`.
//!
//! | Kolom CQL       | Tipe CQL | Rust            |
//! |-----------------|----------|-----------------|
//! | name (PK)       | text     | String          |
//! | description     | text     | String          |
//! | urutan_tampil   | int      | Option\<i32\>   |
//! | phase           | text     | String (nama enum PhaseWyckoff) |

use scylla::DeserializeRow;

use crate::{PhaseWyckoff, WyckoffGlossaryRow};

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
    pub urutan_tampil: Option<i32>,
    /// Nama enum: `ACCUMULATION` / `DISTRIBUTION` / kosong.
    #[scylla(default_when_null)]
    pub phase: String,
}

pub fn phase_to_proto(phase: &str) -> i32 {
    match phase.trim().to_ascii_uppercase().as_str() {
        "ACCUMULATION" => PhaseWyckoff::Accumulation as i32,
        "DISTRIBUTION" => PhaseWyckoff::Distribution as i32,
        _ => PhaseWyckoff::Unspecified as i32,
    }
}

pub fn phase_from_proto(phase: i32) -> String {
    match PhaseWyckoff::try_from(phase).unwrap_or(PhaseWyckoff::Unspecified) {
        PhaseWyckoff::Accumulation => "ACCUMULATION".to_string(),
        PhaseWyckoff::Distribution => "DISTRIBUTION".to_string(),
        PhaseWyckoff::Unspecified => String::new(),
    }
}

impl WyckoffGlossary {
    pub fn into_proto(self) -> WyckoffGlossaryRow {
        WyckoffGlossaryRow {
            name: self.name,
            description: self.description,
            phase: phase_to_proto(&self.phase),
            urutan_tampil: self.urutan_tampil.unwrap_or(0),
        }
    }
}
