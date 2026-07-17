//! Model Scylla untuk tabel `stockbit.emiten_list` + UDT terkait
//! (`emiten_shareholder_gt1`, `emiten_shareholder`, `company_profile`).

use chrono::{DateTime, Utc};
use scylla::{DeserializeRow, DeserializeValue, SerializeValue};
use std::collections::HashMap;

use crate::{
    CompanyProfile as ProtoCompanyProfile, EmitenListRow, EmitenShareholder as ProtoShareholder,
    EmitenShareholderGt1 as ProtoShareholderGt1, StringMap,
};

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

fn nested_map_to_proto(
    m: HashMap<String, HashMap<String, String>>,
) -> HashMap<String, StringMap> {
    m.into_iter()
        .map(|(k, v)| (k, StringMap { entries: v }))
        .collect()
}

/// Baris tabel dasar `emiten_list`.
/// PK: `(("code_name"))`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct EmitenList {
    #[scylla(default_when_null)]
    pub code_name: String,
    #[scylla(default_when_null)]
    pub long_name: String,
    #[scylla(default_when_null)]
    pub key_stats: HashMap<String, String>,
    #[scylla(default_when_null)]
    pub income_statement_ttm: HashMap<String, HashMap<String, String>>,
    #[scylla(default_when_null)]
    pub balance_sheet_quarterly: HashMap<String, HashMap<String, String>>,
    #[scylla(default_when_null)]
    pub cash_flow_ttm: HashMap<String, HashMap<String, String>>,
    #[scylla(default_when_null)]
    pub corporate_action: HashMap<String, HashMap<String, String>>,
    #[scylla(default_when_null)]
    pub shareholder: Vec<String>,
    pub company_profile: Option<CompanyProfile>,
    pub update_at: Option<DateTime<Utc>>,
}

impl EmitenList {
    pub fn into_proto(self) -> EmitenListRow {
        EmitenListRow {
            code_name: self.code_name,
            long_name: self.long_name,
            key_stats: self.key_stats,
            income_statement_ttm: nested_map_to_proto(self.income_statement_ttm),
            balance_sheet_quarterly: nested_map_to_proto(self.balance_sheet_quarterly),
            cash_flow_ttm: nested_map_to_proto(self.cash_flow_ttm),
            corporate_action: nested_map_to_proto(self.corporate_action),
            shareholder: self.shareholder,
            company_profile: self.company_profile.map(CompanyProfile::into_proto),
            update_at: self
                .update_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}
