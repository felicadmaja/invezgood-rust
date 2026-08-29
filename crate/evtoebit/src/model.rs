//! Model baris tabel `invezgood.evtoebit`.

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, SerializeRow};

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "evtoebit";

/// Satu baris agregat median EV/EBIT per sektor BEI.
/// PK: `sektor`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct EvToEbitRow {
    pub sektor: String,
    pub n: i32,
    pub median_ev_ebit: f64,
    pub p25_ev_ebit: f64,
    pub p75_ev_ebit: f64,
    pub median_ev_ebitda: f64,
    #[scylla(default_when_null)]
    pub flag: Option<String>,
    pub updated_at: DateTime<Utc>,
}
