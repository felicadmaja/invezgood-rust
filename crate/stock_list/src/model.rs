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

/// UDT `balance_statement_row` — urutan field = (id, name, level, values, parent_id, is_abstract, display_order).
pub type BalanceStatementRowDb = (
    String,
    String,
    i32,
    Option<Vec<KeystatsValueDb>>,
    Option<String>,
    bool,
    i32,
);

/// UDT `stock_list_balance_statement` — urutan field = (rows, columns).
pub type StockListBalanceStatementDb = (
    Option<Vec<BalanceStatementRowDb>>,
    Option<Vec<KeystatsColumnDb>>,
);

/// UDT `stock_list_income_statement` — struktur sama dengan balance_statement.
pub type StockListIncomeStatementDb = StockListBalanceStatementDb;

/// UDT `stock_list_cash_flow` — struktur sama dengan balance_statement.
pub type StockListCashFlowDb = StockListBalanceStatementDb;

/// UDT `share_holder_5_entry` — urutan field = (name, date, val, percent).
pub type ShareHolder5EntryDb = (String, chrono::DateTime<chrono::Utc>, String, f64);

/// Kolom `share_holder_5` — list entri pemegang saham >1%.
pub type ShareHolder5Db = Option<Vec<ShareHolder5EntryDb>>;

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
    #[scylla(default_when_null)]
    pub keystats_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub balance_statement: Option<StockListBalanceStatementDb>,
    #[scylla(default_when_null)]
    pub balance_statement_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub income_statement: Option<StockListIncomeStatementDb>,
    #[scylla(default_when_null)]
    pub income_statement_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub cash_flow: Option<StockListCashFlowDb>,
    #[scylla(default_when_null)]
    pub cash_flow_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub share_holder_5: ShareHolder5Db,
    #[scylla(default_when_null)]
    pub share_holder_5_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolder5 {
    pub items: Vec<ShareHolder5Entry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolder5Entry {
    pub name: String,
    pub date: chrono::DateTime<chrono::Utc>,
    pub val: String,
    pub percent: f64,
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

#[derive(Debug, Clone)]
pub struct BalanceStatementRow {
    pub id: String,
    pub name: String,
    pub level: i32,
    pub values: Vec<KeystatsValue>,
    pub parent_id: Option<String>,
    pub is_abstract: bool,
    pub display_order: i32,
}

#[derive(Debug, Clone)]
pub struct BalanceStatement {
    pub rows: Vec<BalanceStatementRow>,
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

impl From<BalanceStatementRowDb> for BalanceStatementRow {
    fn from(
        (id, name, level, values, parent_id, is_abstract, display_order): BalanceStatementRowDb,
    ) -> Self {
        Self {
            id,
            name,
            level,
            values: values
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsValue::from)
                .collect(),
            parent_id,
            is_abstract,
            display_order,
        }
    }
}

impl From<BalanceStatementRow> for BalanceStatementRowDb {
    fn from(r: BalanceStatementRow) -> Self {
        (
            r.id,
            r.name,
            r.level,
            Some(r.values.into_iter().map(KeystatsValueDb::from).collect()),
            r.parent_id,
            r.is_abstract,
            r.display_order,
        )
    }
}

impl From<StockListBalanceStatementDb> for BalanceStatement {
    fn from((rows, columns): StockListBalanceStatementDb) -> Self {
        Self {
            rows: rows
                .unwrap_or_default()
                .into_iter()
                .map(BalanceStatementRow::from)
                .collect(),
            columns: columns
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsColumn::from)
                .collect(),
        }
    }
}

impl From<BalanceStatement> for StockListBalanceStatementDb {
    fn from(b: BalanceStatement) -> Self {
        (
            Some(
                b.rows
                    .into_iter()
                    .map(BalanceStatementRowDb::from)
                    .collect(),
            ),
            Some(b.columns.into_iter().map(KeystatsColumnDb::from).collect()),
        )
    }
}

impl From<ShareHolder5EntryDb> for ShareHolder5Entry {
    fn from((name, date, val, percent): ShareHolder5EntryDb) -> Self {
        Self {
            name,
            date,
            val,
            percent,
        }
    }
}

impl From<ShareHolder5Entry> for ShareHolder5EntryDb {
    fn from(e: ShareHolder5Entry) -> Self {
        (e.name, e.date, e.val, e.percent)
    }
}

impl From<ShareHolder5Db> for ShareHolder5 {
    fn from(entries: ShareHolder5Db) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolder5Entry::from)
                .collect(),
        }
    }
}

impl From<ShareHolder5> for ShareHolder5Db {
    fn from(entries: ShareHolder5) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolder5EntryDb::from)
                .collect(),
        )
    }
}
