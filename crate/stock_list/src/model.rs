//! Model baris tabel `invezgood.stock_list`.

use scylla::DeserializeRow;
use scylla::DeserializeValue;
use scylla::SerializeRow;
use scylla::SerializeValue;

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

/// UDT `share_holder_1_entry` — urutan field = (name, holder_type, status, nationality, domicile, scripless, scrip, total, percentage).
pub type ShareHolder1EntryDb = (String, String, String, String, String, String, String, String, f64);

/// Kolom `share_holder_1` — list entri pemegang saham detail >1%.
pub type ShareHolder1Db = Option<Vec<ShareHolder1EntryDb>>;

/// UDT `share_holder_composition_entry` — urutan field = (name, percentage, badge).
pub type ShareHolderCompositionEntryDb = (String, f64, String);

/// Kolom `share_holder_composition` — komposisi kepemilikan (pengendali, direksi, dll.).
pub type ShareHolderCompositionDb = Option<Vec<ShareHolderCompositionEntryDb>>;

/// UDT `company_person_entry` — urutan field = (name, position).
pub type CompanyPersonEntryDb = (String, String);

/// UDT `company_subsidiary_entry` — urutan field = (name, percentage).
pub type CompanySubsidiaryEntryDb = (String, f64);

/// UDT `company_information` — profil perusahaan dari API /analysis/information/{code}.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CompanyInformationDb {
    #[scylla(default_when_null)]
    pub address: Option<String>,
    #[scylla(default_when_null)]
    pub industry: Option<String>,
    #[scylla(default_when_null)]
    pub subsindustry: Option<String>,
    #[scylla(default_when_null)]
    pub activity: Option<String>,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub npwp: Option<String>,
    #[scylla(default_when_null)]
    pub board: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub subsector: Option<String>,
    #[scylla(default_when_null)]
    pub listing_date: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub website: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub additional_info: Option<String>,
    #[scylla(default_when_null)]
    pub people: Option<String>,
    #[scylla(default_when_null)]
    pub report_type: Option<String>,
    #[scylla(default_when_null)]
    pub administration: Option<String>,
    #[scylla(default_when_null)]
    pub description: Option<String>,
    #[scylla(default_when_null)]
    pub ipo_pct: Option<f64>,
    #[scylla(default_when_null)]
    pub ipo_price: Option<f64>,
    #[scylla(default_when_null)]
    pub ipo_share: Option<String>,
    #[scylla(default_when_null)]
    pub ipo_underwriter: Option<String>,
    #[scylla(default_when_null)]
    pub nominal_price: Option<f64>,
    #[scylla(default_when_null)]
    pub category: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub active: Option<bool>,
    #[scylla(default_when_null)]
    pub commissioner: Option<Vec<CompanyPersonEntryDb>>,
    #[scylla(default_when_null)]
    pub director: Option<Vec<CompanyPersonEntryDb>>,
    #[scylla(default_when_null)]
    pub subsidiary: Option<Vec<CompanySubsidiaryEntryDb>>,
}

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
    #[scylla(default_when_null)]
    pub share_holder_1: ShareHolder1Db,
    #[scylla(default_when_null)]
    pub share_holder_1_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub share_holder_composition: ShareHolderCompositionDb,
    #[scylla(default_when_null)]
    pub share_holder_composition_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub company_information: Option<CompanyInformationDb>,
    #[scylla(default_when_null)]
    pub company_information_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub struct CompanyPersonEntry {
    pub name: String,
    pub position: String,
}

#[derive(Debug, Clone)]
pub struct CompanySubsidiaryEntry {
    pub name: String,
    pub percentage: f64,
}

#[derive(Debug, Clone, Default)]
pub struct CompanyInformation {
    pub address: String,
    pub industry: String,
    pub subsindustry: String,
    pub activity: String,
    pub name: String,
    pub npwp: String,
    pub board: String,
    pub sector: String,
    pub subsector: String,
    pub listing_date: Option<chrono::DateTime<chrono::Utc>>,
    pub website: String,
    pub logo: String,
    pub additional_info: Option<String>,
    pub people: Option<String>,
    pub report_type: Option<String>,
    pub administration: Option<String>,
    pub description: Option<String>,
    pub ipo_pct: Option<f64>,
    pub ipo_price: Option<f64>,
    pub ipo_share: Option<String>,
    pub ipo_underwriter: Option<String>,
    pub nominal_price: Option<f64>,
    pub category: Vec<String>,
    pub active: bool,
    pub commissioner: Vec<CompanyPersonEntry>,
    pub director: Vec<CompanyPersonEntry>,
    pub subsidiary: Vec<CompanySubsidiaryEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolderComposition {
    pub items: Vec<ShareHolderCompositionEntry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolderCompositionEntry {
    pub name: String,
    pub percentage: f64,
    pub badge: String,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolder1 {
    pub items: Vec<ShareHolder1Entry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolder1Entry {
    pub name: String,
    pub holder_type: String,
    pub status: String,
    pub nationality: String,
    pub domicile: String,
    pub scripless: String,
    pub scrip: String,
    pub total: String,
    pub percentage: f64,
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

impl From<ShareHolder1EntryDb> for ShareHolder1Entry {
    fn from(
        (name, holder_type, status, nationality, domicile, scripless, scrip, total, percentage): ShareHolder1EntryDb,
    ) -> Self {
        Self {
            name,
            holder_type,
            status,
            nationality,
            domicile,
            scripless,
            scrip,
            total,
            percentage,
        }
    }
}

impl From<ShareHolder1Entry> for ShareHolder1EntryDb {
    fn from(e: ShareHolder1Entry) -> Self {
        (
            e.name,
            e.holder_type,
            e.status,
            e.nationality,
            e.domicile,
            e.scripless,
            e.scrip,
            e.total,
            e.percentage,
        )
    }
}

impl From<ShareHolder1Db> for ShareHolder1 {
    fn from(entries: ShareHolder1Db) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolder1Entry::from)
                .collect(),
        }
    }
}

impl From<ShareHolder1> for ShareHolder1Db {
    fn from(entries: ShareHolder1) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolder1EntryDb::from)
                .collect(),
        )
    }
}

impl From<ShareHolderCompositionEntryDb> for ShareHolderCompositionEntry {
    fn from((name, percentage, badge): ShareHolderCompositionEntryDb) -> Self {
        Self {
            name,
            percentage,
            badge,
        }
    }
}

impl From<ShareHolderCompositionEntry> for ShareHolderCompositionEntryDb {
    fn from(e: ShareHolderCompositionEntry) -> Self {
        (e.name, e.percentage, e.badge)
    }
}

impl From<ShareHolderCompositionDb> for ShareHolderComposition {
    fn from(entries: ShareHolderCompositionDb) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolderCompositionEntry::from)
                .collect(),
        }
    }
}

impl From<ShareHolderComposition> for ShareHolderCompositionDb {
    fn from(entries: ShareHolderComposition) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolderCompositionEntryDb::from)
                .collect(),
        )
    }
}

impl From<CompanyPersonEntryDb> for CompanyPersonEntry {
    fn from((name, position): CompanyPersonEntryDb) -> Self {
        Self { name, position }
    }
}

impl From<CompanyPersonEntry> for CompanyPersonEntryDb {
    fn from(e: CompanyPersonEntry) -> Self {
        (e.name, e.position)
    }
}

impl From<CompanySubsidiaryEntryDb> for CompanySubsidiaryEntry {
    fn from((name, percentage): CompanySubsidiaryEntryDb) -> Self {
        Self { name, percentage }
    }
}

impl From<CompanySubsidiaryEntry> for CompanySubsidiaryEntryDb {
    fn from(e: CompanySubsidiaryEntry) -> Self {
        (e.name, e.percentage)
    }
}

impl From<CompanyInformationDb> for CompanyInformation {
    fn from(db: CompanyInformationDb) -> Self {
        Self {
            address: db.address.unwrap_or_default(),
            industry: db.industry.unwrap_or_default(),
            subsindustry: db.subsindustry.unwrap_or_default(),
            activity: db.activity.unwrap_or_default(),
            name: db.name.unwrap_or_default(),
            npwp: db.npwp.unwrap_or_default(),
            board: db.board.unwrap_or_default(),
            sector: db.sector.unwrap_or_default(),
            subsector: db.subsector.unwrap_or_default(),
            listing_date: db.listing_date,
            website: db.website.unwrap_or_default(),
            logo: db.logo.unwrap_or_default(),
            additional_info: db.additional_info,
            people: db.people,
            report_type: db.report_type,
            administration: db.administration,
            description: db.description,
            ipo_pct: db.ipo_pct,
            ipo_price: db.ipo_price,
            ipo_share: db.ipo_share,
            ipo_underwriter: db.ipo_underwriter,
            nominal_price: db.nominal_price,
            category: db.category.unwrap_or_default(),
            active: db.active.unwrap_or(false),
            commissioner: db
                .commissioner
                .unwrap_or_default()
                .into_iter()
                .map(CompanyPersonEntry::from)
                .collect(),
            director: db
                .director
                .unwrap_or_default()
                .into_iter()
                .map(CompanyPersonEntry::from)
                .collect(),
            subsidiary: db
                .subsidiary
                .unwrap_or_default()
                .into_iter()
                .map(CompanySubsidiaryEntry::from)
                .collect(),
        }
    }
}

impl From<CompanyInformation> for CompanyInformationDb {
    fn from(info: CompanyInformation) -> Self {
        Self {
            address: Some(info.address),
            industry: Some(info.industry),
            subsindustry: Some(info.subsindustry),
            activity: Some(info.activity),
            name: Some(info.name),
            npwp: Some(info.npwp),
            board: Some(info.board),
            sector: Some(info.sector),
            subsector: Some(info.subsector),
            listing_date: info.listing_date,
            website: Some(info.website),
            logo: Some(info.logo),
            additional_info: info.additional_info,
            people: info.people,
            report_type: info.report_type,
            administration: info.administration,
            description: info.description,
            ipo_pct: info.ipo_pct,
            ipo_price: info.ipo_price,
            ipo_share: info.ipo_share,
            ipo_underwriter: info.ipo_underwriter,
            nominal_price: info.nominal_price,
            category: Some(info.category),
            active: Some(info.active),
            commissioner: Some(
                info.commissioner
                    .into_iter()
                    .map(CompanyPersonEntryDb::from)
                    .collect(),
            ),
            director: Some(
                info.director
                    .into_iter()
                    .map(CompanyPersonEntryDb::from)
                    .collect(),
            ),
            subsidiary: Some(
                info.subsidiary
                    .into_iter()
                    .map(CompanySubsidiaryEntryDb::from)
                    .collect(),
            ),
        }
    }
}
