//! Model Scylla untuk tabel `stockbit.rule` (lihat `rule.cql`).
//!
//! | Kolom CQL           | Tipe CQL | Rust  |
//! |---------------------|----------|-------|
//! | rule_name (PK)      | text     | String|
//! | rule_description    | text     | String|
//! | rule_parameter      | text     | String|
//! | rule_gt_or_lt       | tinyint  | i8    |
//! | rule_value          | double   | f64   |
//!
//! Nilai `rule_gt_or_lt` (proto `RuleGtOrLt`):
//! -2 ≤, -1 <, 0 =, 1 >, 2 ≥.

use scylla::{DeserializeRow, SerializeValue};

/// Baris tabel dasar `rule`.
/// PK: `(("rule_name"))`.
#[derive(Debug, Clone, DeserializeRow, SerializeValue)]
pub struct Rule {
    #[scylla(default_when_null)]
    pub rule_name: String,
    #[scylla(default_when_null)]
    pub rule_description: String,
    #[scylla(default_when_null)]
    pub rule_parameter: String,
    /// CQL `tinyint` — lihat enum `RuleGtOrLt` di `rule.proto`.
    pub rule_gt_or_lt: i8,
    pub rule_value: f64,
}
