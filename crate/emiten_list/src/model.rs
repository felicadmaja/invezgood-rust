//! Model Scylla untuk tabel `stockbit.emiten_list` + UDT terkait
//! (`emiten_shareholder_gt1`, `emiten_shareholder`, `company_profile`).
//! Lihat `emiten_list.cql`.
//!
//! | Kolom CQL         | Tipe CQL | Rust |
//! |-------------------|----------|------|
//! | emiten_name (PK)    | text     | String |
//! | long_name         | text     | String |
//! | emiten_icon       | text     | String |
//! | key_stats         | map\<text, text\> | HashMap\<String, String\> |
//! | corporate_action  | list\<frozen\<map\<...\>\>\> | Vec\<HashMap\<...\>\> |
//! | company_profile   | frozen\<company_profile\> | Option\<CompanyProfile\> |
//! | update_at              | timestamp | Option\<DateTime\<Utc\>\> |
//! | is_konglomerasi        | boolean  | bool (default false) |
//! | sector                 | tinyint  | Option\<i8\> |
//! | is_fundamental_solid   | boolean  | bool (default false) |
//! | is_blue_chip           | boolean  | bool (default false) |
//! | is_plan_to_trade       | boolean  | bool (default false) |
//! | catatan                | map\<text, text\> | HashMap\<String, String\> |
//! | catatan_owner          | text     | String |
//! | foto_owner             | list\<text\> | Vec\<String\> |
//! | net_income             | map\<text, frozen\<map\<text, text\>\>\> | HashMap\<String, HashMap\<String, String\>\> |
//! | takeprofit_wyckoff     | map\<text, text\> | HashMap\<String, String\> |

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};
use std::collections::HashMap;

use crate::{
    CompanyProfile as ProtoCompanyProfile, CorporateActionDetailList, CorporateActionGroup,
    CorporateActionKv, EmitenListRow, EmitenShareholder as ProtoShareholder,
    EmitenShareholderGt1 as ProtoShareholderGt1, NetIncomeYear,
};

fn sector_to_proto(sector: Option<i8>) -> i32 {
    sector.unwrap_or(0).max(0) as i32
}

/// UDT `emiten_shareholder_gt1` — pemegang saham >1%.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct EmitenShareholderGt1 {
    #[scylla(default_when_null)]
    pub name: String,
    #[scylla(rename = "type", default_when_null)]
    pub type_: String,
    #[scylla(default_when_null)]
    pub location: String,
    #[scylla(default_when_null)]
    pub domicile: String,
    #[scylla(default_when_null)]
    pub scriples: String,
    #[scylla(default_when_null)]
    pub scrip: String,
    #[scylla(default_when_null)]
    pub total_shares: String,
    #[scylla(default_when_null)]
    pub percentage: String,
}

impl EmitenShareholderGt1 {
    pub fn into_proto(self) -> ProtoShareholderGt1 {
        ProtoShareholderGt1 {
            name: self.name,
            r#type: self.type_,
            location: self.location,
            domicile: self.domicile,
            scriples: self.scriples,
            scrip: self.scrip,
            total_shares: self.total_shares,
            percentage: self.percentage,
        }
    }
}

/// UDT `emiten_shareholder` — ringkasan pemegang saham.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct EmitenShareholder {
    #[scylla(default_when_null)]
    pub name: String,
    #[scylla(default_when_null)]
    pub value: String,
    #[scylla(default_when_null)]
    pub shares: String,
}

impl EmitenShareholder {
    pub fn into_proto(self) -> ProtoShareholder {
        ProtoShareholder {
            name: self.name,
            value: self.value,
            shares: self.shares,
        }
    }
}

/// UDT `company_profile`.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CompanyProfile {
    #[scylla(default_when_null)]
    pub company_background: String,
    #[scylla(default_when_null)]
    pub sector: String,
    #[scylla(default_when_null)]
    pub shareholder_more_than_one_percent: Vec<EmitenShareholderGt1>,
    #[scylla(default_when_null)]
    pub shareholders: Vec<EmitenShareholder>,
    #[scylla(default_when_null)]
    pub ultimate_beneficial_owner: String,
}

impl CompanyProfile {
    pub fn into_proto(self) -> ProtoCompanyProfile {
        ProtoCompanyProfile {
            company_background: self.company_background,
            sector: self.sector,
            shareholder_more_than_one_percent: self
                .shareholder_more_than_one_percent
                .into_iter()
                .map(EmitenShareholderGt1::into_proto)
                .collect(),
            shareholders: self
                .shareholders
                .into_iter()
                .map(EmitenShareholder::into_proto)
                .collect(),
            ultimate_beneficial_owner: self.ultimate_beneficial_owner,
        }
    }
}

fn corporate_action_to_proto(
    items: Vec<HashMap<String, Vec<HashMap<String, String>>>>,
) -> Vec<CorporateActionGroup> {
    items
        .into_iter()
        .map(|group| CorporateActionGroup {
            by_type: group
                .into_iter()
                .map(|(action_type, details)| {
                    (
                        action_type,
                        CorporateActionDetailList {
                            items: details
                                .into_iter()
                                .map(|kv| CorporateActionKv { entries: kv })
                                .collect(),
                        },
                    )
                })
                .collect(),
        })
        .collect()
}

/// Baris tabel dasar `emiten_list`.
/// PK: `(("emiten_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct EmitenList {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub long_name: String,
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    #[scylla(default_when_null)]
    pub key_stats: HashMap<String, String>,
    /// Bentuk: `[{"Dividend":[{"Dividend":"Rp 209"},{"Cum Date":"..."},...]}, ...]`
    #[scylla(default_when_null)]
    pub corporate_action: Vec<HashMap<String, Vec<HashMap<String, String>>>>,
    pub company_profile: Option<CompanyProfile>,
    pub update_at: Option<DateTime<Utc>>,
    #[scylla(default_when_null)]
    pub is_konglomerasi: bool,
    pub sector: Option<i8>,
    #[scylla(default_when_null)]
    pub is_fundamental_solid: bool,
    #[scylla(default_when_null)]
    pub is_blue_chip: bool,
    #[scylla(default_when_null)]
    pub is_plan_to_trade: bool,
    /// Catatan manual: map key-value.
    #[scylla(default_when_null)]
    pub catatan: HashMap<String, String>,
    /// Pemilik/penulis catatan.
    #[scylla(default_when_null)]
    pub catatan_owner: String,
    /// Path/URL foto pemilik catatan.
    #[scylla(default_when_null)]
    pub foto_owner: Vec<String>,
    /// Tahun → { Q1/Q2/... → nilai teks }.
    #[scylla(default_when_null)]
    pub net_income: HashMap<String, HashMap<String, String>>,
    /// Take-profit / Wyckoff: map key-value (nilai disimpan sebagai text).
    #[scylla(default_when_null)]
    pub takeprofit_wyckoff: HashMap<String, String>,
}

impl EmitenList {
    pub fn into_proto(self) -> EmitenListRow {
        EmitenListRow {
            emiten_name: self.emiten_name,
            long_name: self.long_name,
            emiten_icon: self.emiten_icon,
            key_stats: self.key_stats,
            corporate_action: corporate_action_to_proto(self.corporate_action),
            company_profile: self.company_profile.map(CompanyProfile::into_proto),
            update_at: self
                .update_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
            is_konglomerasi: self.is_konglomerasi,
            sector: sector_to_proto(self.sector),
            is_fundamental_solid: self.is_fundamental_solid,
            is_blue_chip: self.is_blue_chip,
            is_plan_to_trade: self.is_plan_to_trade,
            catatan: self.catatan,
            catatan_owner: self.catatan_owner,
            foto_owner: self.foto_owner,
            net_income: self
                .net_income
                .into_iter()
                .map(|(year, periods)| (year, NetIncomeYear { periods }))
                .collect(),
            takeprofit_wyckoff: self.takeprofit_wyckoff,
        }
    }
}
