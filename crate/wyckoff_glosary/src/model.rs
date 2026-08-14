//! Model Scylla untuk tabel `invezgood.wyckoff_glossary`.
//! Lihat `wyckoff_glossary.cql`.
//!
//! | Kolom CQL       | Tipe CQL | Rust            |
//! |-----------------|----------|-----------------|
//! | name (PK)       | text     | String          |
//! | long_name       | text     | String          |
//! | description     | text     | String          |
//! | urutan_tampil   | int      | Option\<i32\>   |
//! | jenis           | text     | String (nama enum JenisWyckoff) |
//! | phase           | text     | String (nama enum PhaseWyckoff) |

use scylla::DeserializeRow;

use crate::{JenisWyckoff, PhaseWyckoff, WyckoffGlossaryRow};

/// Baris tabel dasar `wyckoff_glossary`.
/// PK: `(("name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct WyckoffGlossary {
    /// Partition key — istilah / nama entri glosarium.
    #[scylla(default_when_null)]
    pub name: String,
    /// Nama panjang / label tampilan; kosong bila belum di-set.
    #[scylla(default_when_null)]
    pub long_name: String,
    /// Penjelasan istilah; kosong bila belum di-set.
    #[scylla(default_when_null)]
    pub description: String,
    pub urutan_tampil: Option<i32>,
    /// Nama enum: `ACCUMULATION` / `DISTRIBUTION` / kosong.
    #[scylla(default_when_null)]
    pub jenis: String,
    /// Nama enum: `A` / `B` / `C` / `D` / `E` / kosong.
    #[scylla(default_when_null)]
    pub phase: String,
}

pub fn jenis_to_proto(jenis: &str) -> i32 {
    match jenis.trim().to_ascii_uppercase().as_str() {
        "ACCUMULATION" => JenisWyckoff::Accumulation as i32,
        "DISTRIBUTION" => JenisWyckoff::Distribution as i32,
        _ => JenisWyckoff::Unspecified as i32,
    }
}

pub fn jenis_from_proto(jenis: i32) -> String {
    match JenisWyckoff::try_from(jenis).unwrap_or(JenisWyckoff::Unspecified) {
        JenisWyckoff::Accumulation => "ACCUMULATION".to_string(),
        JenisWyckoff::Distribution => "DISTRIBUTION".to_string(),
        JenisWyckoff::Unspecified => String::new(),
    }
}

pub fn phase_to_proto(phase: &str) -> i32 {
    match phase.trim().to_ascii_uppercase().as_str() {
        "A" => PhaseWyckoff::A as i32,
        "B" => PhaseWyckoff::B as i32,
        "C" => PhaseWyckoff::C as i32,
        "D" => PhaseWyckoff::D as i32,
        "E" => PhaseWyckoff::E as i32,
        _ => PhaseWyckoff::Unspecified as i32,
    }
}

pub fn phase_from_proto(phase: i32) -> String {
    match PhaseWyckoff::try_from(phase).unwrap_or(PhaseWyckoff::Unspecified) {
        PhaseWyckoff::A => "A".to_string(),
        PhaseWyckoff::B => "B".to_string(),
        PhaseWyckoff::C => "C".to_string(),
        PhaseWyckoff::D => "D".to_string(),
        PhaseWyckoff::E => "E".to_string(),
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
            long_name: self.long_name,
            jenis: jenis_to_proto(&self.jenis),
        }
    }
}
