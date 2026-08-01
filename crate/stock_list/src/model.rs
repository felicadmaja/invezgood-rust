//! Model baris tabel `invezgood.stock_list`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "stock_list";

/// UDT `keystats_value` — urutan field = (col, year, amount, period).
pub type KeystatsValueDb = (String, i32, f64, String);

/// UDT `keystats_column` — urutan field = (year, label, period).
pub type KeystatsColumnDb = (i32, String, String);

/// UDT `keystats_row` — urutan field = (id, name, values).
pub type KeystatsRowDb = (String, String, Option<Vec<KeystatsValueDb>>);

/// UDT `stock_list_keystats` — urutan field = (rows, columns).
pub type StockListKeystatsDb = (
    Option<Vec<KeystatsRowDb>>,
    Option<Vec<KeystatsColumnDb>>,
);

/// Satu baris `invezgood.stock_list`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct StockListRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub keystats: Option<StockListKeystatsDb>,
}

#[derive(Debug, Clone)]
pub struct KeystatsValue {
    pub col: String,
    pub year: i32,
    pub amount: f64,
    pub period: String,
}

#[derive(Debug, Clone)]
pub struct KeystatsRow {
    pub id: String,
    pub name: String,
    pub values: Vec<KeystatsValue>,
}

#[derive(Debug, Clone)]
pub struct KeystatsColumn {
    pub year: i32,
    pub label: String,
    pub period: String,
}

#[derive(Debug, Clone)]
pub struct Keystats {
    pub rows: Vec<KeystatsRow>,
    pub columns: Vec<KeystatsColumn>,
}

impl From<KeystatsValueDb> for KeystatsValue {
    fn from((col, year, amount, period): KeystatsValueDb) -> Self {
        Self {
            col,
            year,
            amount,
            period,
        }
    }
}

impl From<KeystatsValue> for KeystatsValueDb {
    fn from(v: KeystatsValue) -> Self {
        (v.col, v.year, v.amount, v.period)
    }
}

impl From<KeystatsColumnDb> for KeystatsColumn {
    fn from((year, label, period): KeystatsColumnDb) -> Self {
        Self {
            year,
            label,
            period,
        }
    }
}

impl From<KeystatsColumn> for KeystatsColumnDb {
    fn from(c: KeystatsColumn) -> Self {
        (c.year, c.label, c.period)
    }
}

impl From<KeystatsRowDb> for KeystatsRow {
    fn from((id, name, values): KeystatsRowDb) -> Self {
        Self {
            id,
            name,
            values: values
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsValue::from)
                .collect(),
        }
    }
}

impl From<KeystatsRow> for KeystatsRowDb {
    fn from(r: KeystatsRow) -> Self {
        (
            r.id,
            r.name,
            Some(r.values.into_iter().map(KeystatsValueDb::from).collect()),
        )
    }
}

impl From<StockListKeystatsDb> for Keystats {
    fn from((rows, columns): StockListKeystatsDb) -> Self {
        Self {
            rows: rows
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsRow::from)
                .collect(),
            columns: columns
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsColumn::from)
                .collect(),
        }
    }
}

impl From<Keystats> for StockListKeystatsDb {
    fn from(k: Keystats) -> Self {
        (
            Some(k.rows.into_iter().map(KeystatsRowDb::from).collect()),
            Some(k.columns.into_iter().map(KeystatsColumnDb::from).collect()),
        )
    }
}
