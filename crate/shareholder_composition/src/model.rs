//! Model baris tabel `invezgood.shareholder_composition`.

use std::collections::HashMap;

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "shareholder_composition";
pub const MV_BY_CODE: &str = "shareholder_composition_by_code";

/// Baris PK-only MV `shareholder_composition_by_code`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct ShareholderCompositionPkRow {
    pub code: String,
    pub tahun_bulan: String,
}

/// Satu baris `invezgood.shareholder_composition`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct ShareholderCompositionRow {
    pub code: String,
    pub tahun_bulan: String,
    #[scylla(default_when_null)]
    pub detail: Option<HashMap<String, String>>,
}
