//! Key Stats + Corp. Action + Profile via API → upsert `emiten_list`.
//! Bearer dari sesi browser setelah login (Chrome hanya untuk token).

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use chromiumoxide::page::Page;
use gcs::{download_and_upload_emiten_icon, GcsOAuthTokenCache, GcsSignedUrlRuntime};
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde::Deserialize;
use serde_json::Value;
use stockbit_browser::extract_stockbit_bearer;

const KEYSTATS_RATIO_URL: &str = "https://exodus.stockbit.com/keystats/ratio/v1";
const KEYSTATS_YEAR_LIMIT: u32 = 10;
const CORPACTION_URL: &str = "https://exodus.stockbit.com/corpaction";
const CORPACTION_LIMIT: u32 = 30;
const PROFILE_URL: &str = "https://exodus.stockbit.com/emitten";
const SEARCH_URL: &str = "https://exodus.stockbit.com/search";
const EMITEN_ICON_ASSETS_BASE: &str = "https://assets.stockbit.com/logos/companies";

pub const UPDATE_AT_FRESH_DAYS: i64 = 30;

/// `true` bila perlu scrape ulang: `update_at` kosong atau usia ≥ [`UPDATE_AT_FRESH_DAYS`].
pub fn is_emiten_update_at_stale(update_at: Option<DateTime<Utc>>) -> bool {
    match update_at {
        None => true,
        Some(ts) => Utc::now().signed_duration_since(ts) >= ChronoDuration::days(UPDATE_AT_FRESH_DAYS),
    }
}

/// Bentuk Scylla `corporate_action`:
/// `[{"Dividend":[{"Dividend":"Rp 209"},{"Cum Date":"..."},...]}, ...]`
type CorporateAction = Vec<HashMap<String, Vec<HashMap<String, String>>>>;

/// Bentuk Scylla `net_income`: tahun → periode → nilai (`map<text, frozen<map<text, text>>>`).
type NetIncome = HashMap<String, HashMap<String, String>>;

/// UDT `emiten_shareholder_gt1`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct EmitenShareholderGt1 {
    pub name: String,
    #[scylla(rename = "type")]
    #[serde(rename = "type")]
    pub type_: String,
    pub location: String,
    pub domicile: String,
    pub scriples: String,
    pub scrip: String,
    pub total_shares: String,
    pub percentage: String,
}

/// UDT `emiten_shareholder`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct EmitenShareholder {
    pub name: String,
    pub value: String,
    pub shares: String,
}

/// UDT `company_profile`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct CompanyProfile {
    pub company_background: String,
    pub sector: String,
    pub shareholder_more_than_one_percent: Vec<EmitenShareholderGt1>,
    pub shareholders: Vec<EmitenShareholder>,
    pub ultimate_beneficial_owner: String,
}

fn format_elapsed(started: Instant) -> String {
    let ms = started.elapsed().as_millis();
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

#[derive(Debug, DeserializeRow)]
struct EmitenUpdateAtRow {
    update_at: Option<DateTime<Utc>>,
    #[scylla(default_when_null)]
    emiten_icon: String,
}

#[derive(Debug, DeserializeRow)]
struct EmitenIconRow {
    #[scylla(default_when_null)]
    emiten_icon: String,
}

struct KeyStatsApiResult {
    key_stats: HashMap<String, String>,
    net_income: NetIncome,
}

fn map_stats_field(stats: &Value, api_key: &str, label: &str, out: &mut HashMap<String, String>) {
    if let Some(v) = stats.get(api_key).and_then(|x| x.as_str()) {
        out.insert(label.to_string(), v.trim().to_string());
    }
}

/// Parse response keystats/ratio → `key_stats` map + `net_income` (Period IDR / Net Income).
fn parse_keystats_ratio_json(v: &Value) -> KeyStatsApiResult {
    let mut key_stats = HashMap::new();

    if let Some(groups) = v
        .pointer("/data/closure_fin_items_results")
        .and_then(|x| x.as_array())
    {
        for group in groups {
            let Some(items) = group.get("fin_name_results").and_then(|x| x.as_array()) else {
                continue;
            };
            for item in items {
                let name = item
                    .pointer("/fitem/name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim();
                if name.is_empty() {
                    continue;
                }
                let value = item
                    .pointer("/fitem/value")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                key_stats.insert(name.to_string(), value);
            }
        }
    }

    if let Some(stats) = v.pointer("/data/stats") {
        map_stats_field(stats, "market_cap", "Market Cap", &mut key_stats);
        map_stats_field(stats, "enterprise_value", "Enterprise Value", &mut key_stats);
        map_stats_field(
            stats,
            "current_share_outstanding",
            "Current Share Outstanding",
            &mut key_stats,
        );
        map_stats_field(stats, "free_float", "Free Float", &mut key_stats);
    }

    let mut net_income = NetIncome::new();
    if let Some(groups) = v
        .pointer("/data/financial_year_parent/financial_year_groups")
        .and_then(|x| x.as_array())
    {
        for group in groups {
            if group.get("fitem_name").and_then(|x| x.as_str()) != Some("Net Income") {
                continue;
            }
            let ttm_label = group
                .pointer("/most_recent_quarter/quarter")
                .and_then(|x| x.as_str())
                .map(|q| format!("TTM ({q})"))
                .unwrap_or_else(|| "TTM".to_string());

            let Some(years) = group.get("financial_year_values").and_then(|x| x.as_array()) else {
                break;
            };
            for yv in years {
                let year = yv
                    .get("year")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim();
                if year.is_empty() {
                    continue;
                }
                let mut period_map = HashMap::new();
                if let Some(pvs) = yv.get("period_values").and_then(|x| x.as_array()) {
                    for pv in pvs {
                        let period = pv
                            .get("period")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .trim();
                        if period.is_empty() {
                            continue;
                        }
                        let val = pv
                            .get("quarter_value")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .trim()
                            .to_string();
                        period_map.insert(period.to_string(), val);
                    }
                }
                if let Some(a) = yv.get("annualised_value").and_then(|x| x.as_str()) {
                    period_map.insert("Annualised".to_string(), a.trim().to_string());
                }
                if let Some(t) = yv.get("ttm_value").and_then(|x| x.as_str()) {
                    period_map.insert(ttm_label.clone(), t.trim().to_string());
                }
                net_income.insert(year.to_string(), period_map);
            }
            break;
        }
    }

    KeyStatsApiResult {
        key_stats,
        net_income,
    }
}

async fn fetch_keystats_ratio(
    http: &reqwest::Client,
    bearer: &str,
    code: &str,
) -> Result<KeyStatsApiResult, Box<dyn std::error::Error>> {
    let url = format!("{KEYSTATS_RATIO_URL}/{code}?year_limit={KEYSTATS_YEAR_LIMIT}");
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, &format!("keystats/ratio {code}"));
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("keystats/ratio {code} HTTP {status}: {preview}").into());
    }

    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("keystats/ratio {code} JSON: {e}"))?;
    let parsed = parse_keystats_ratio_json(&v);
    if parsed.key_stats.is_empty() {
        return Err(format!("keystats/ratio {code}: key_stats kosong").into());
    }
    Ok(parsed)
}

fn has_profitability_margins(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Gross Profit Margin (Quarter)")
        && stats.contains_key("Operating Profit Margin (Quarter)")
        && stats.contains_key("Net Profit Margin (Quarter)")
}

fn has_market_cap_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Market Cap")
        && stats.contains_key("Enterprise Value")
        && stats.contains_key("Current Share Outstanding")
        && stats.contains_key("Free Float")
}

fn has_solvency_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Current Ratio (Quarter)")
        && stats.contains_key("Quick Ratio (Quarter)")
        && stats.contains_key("Debt to Equity Ratio (Quarter)")
        && stats.contains_key("LT Debt/Equity (Quarter)")
        && stats.contains_key("Total Liabilities/Equity (Quarter)")
        && stats.contains_key("Total Debt/Total Assets (Quarter)")
        && stats.contains_key("Interest Coverage (TTM)")
}

/// Cash Flow Statement (TTM) dari card Key Stats.
fn has_cash_flow_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Cash From Operations (TTM)")
        && stats.contains_key("Cash From Investing (TTM)")
        && stats.contains_key("Cash From Financing (TTM)")
        && stats.contains_key("Capital expenditure (TTM)")
        && stats.contains_key("Free cash flow (TTM)")
}

fn corp_action_type_label(action_type: &str) -> String {
    match action_type.trim().to_ascii_lowercase().as_str() {
        "rups" => "RUPS".to_string(),
        "dividend" | "dividen" => "Dividend".to_string(),
        "stocksplit" | "stock_split" => "Stock Split".to_string(),
        "rightissue" | "right_issue" | "rightsissue" | "rights_issue" => {
            "Right Issue".to_string()
        }
        "tenderoffer" | "tender_offer" => "Tender Offer".to_string(),
        "bonus" | "bonusshare" | "bonus_share" => "Bonus".to_string(),
        other if other.is_empty() => "Unknown".to_string(),
        other => {
            let mut out = String::new();
            for (i, part) in other.split('_').enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                let mut chars = part.chars();
                if let Some(c) = chars.next() {
                    out.extend(c.to_uppercase());
                    out.extend(chars);
                }
            }
            out
        }
    }
}

fn push_corp_kv(details: &mut Vec<HashMap<String, String>>, key: &str, value: &str) {
    let v = value.trim();
    if v.is_empty() {
        return;
    }
    let mut m = HashMap::new();
    m.insert(key.to_string(), v.replace('\n', " ").split_whitespace().collect::<Vec<_>>().join(" "));
    details.push(m);
}

fn json_field_str(obj: &Value, key: &str) -> String {
    match obj.get(key) {
        Some(Value::String(s)) => s.trim().to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

fn parse_corpaction_item(item: &Value) -> Option<HashMap<String, Vec<HashMap<String, String>>>> {
    let action_type = item
        .get("action_type")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim();
    if action_type.is_empty() {
        return None;
    }
    let label = corp_action_type_label(action_type);
    let info = item.get("action_info")?;
    // payload nested under same key as action_type (rups/stocksplit/dividend/...)
    let payload = info
        .get(action_type)
        .or_else(|| info.get(action_type.to_ascii_lowercase()))
        .or_else(|| {
            info.as_object()
                .and_then(|o| o.values().next())
        })?;

    let mut details = Vec::new();
    match action_type.to_ascii_lowercase().as_str() {
        "rups" => {
            push_corp_kv(&mut details, "Event Date", &json_field_str(payload, "rups_date"));
            push_corp_kv(&mut details, "Time", &json_field_str(payload, "rups_time"));
            push_corp_kv(
                &mut details,
                "Eligible Date",
                &json_field_str(payload, "rups_eligible_date"),
            );
            push_corp_kv(&mut details, "Venue", &json_field_str(payload, "rups_venue"));
        }
        "stocksplit" | "stock_split" => {
            push_corp_kv(
                &mut details,
                "Cum Date",
                &json_field_str(payload, "stocksplit_cumdate"),
            );
            push_corp_kv(
                &mut details,
                "Ex Date",
                &json_field_str(payload, "stocksplit_exdate"),
            );
            push_corp_kv(
                &mut details,
                "Recording Date",
                &json_field_str(payload, "stocksplit_recdate"),
            );
            let old = json_field_str(payload, "stocksplit_old");
            let new = json_field_str(payload, "stocksplit_new");
            if !old.is_empty() && !new.is_empty() {
                push_corp_kv(&mut details, "Ratio", &format!("{old}:{new}"));
            } else {
                push_corp_kv(
                    &mut details,
                    "Factor",
                    &json_field_str(payload, "stocksplit_factor"),
                );
            }
            let price = json_field_str(payload, "stocksplit_new_price");
            if price != "0" {
                push_corp_kv(&mut details, "New Price", &price);
            }
        }
        "dividend" | "dividen" => {
            // Field name bervariasi antar versi API — coba beberapa key umum.
            for (label_k, keys) in [
                ("Dividend", &["dividend", "dividend_amount", "dividen"][..]),
                ("Cum Date", &["cum_date", "cumdate", "dividend_cumdate"][..]),
                ("Ex Date", &["ex_date", "exdate", "dividend_exdate"][..]),
                (
                    "Recording Date",
                    &["rec_date", "recdate", "recording_date", "dividend_recdate"][..],
                ),
                (
                    "Payment Date",
                    &["payment_date", "pay_date", "dividend_payment_date"][..],
                ),
            ] {
                let mut found = String::new();
                for k in keys {
                    found = json_field_str(payload, k);
                    if !found.is_empty() {
                        break;
                    }
                }
                push_corp_kv(&mut details, label_k, &found);
            }
        }
        "rightissue" | "right_issue" | "rightsissue" | "rights_issue" => {
            for (label_k, keys) in [
                ("Cum Date", &["cum_date", "cumdate", "rightissue_cumdate"][..]),
                ("Ex Date", &["ex_date", "exdate", "rightissue_exdate"][..]),
                (
                    "Recording Date",
                    &["rec_date", "recdate", "rightissue_recdate"][..],
                ),
                ("Ratio", &["ratio", "rightissue_ratio"][..]),
                ("Price", &["price", "rightissue_price"][..]),
            ] {
                let mut found = String::new();
                for k in keys {
                    found = json_field_str(payload, k);
                    if !found.is_empty() {
                        break;
                    }
                }
                push_corp_kv(&mut details, label_k, &found);
            }
        }
        _ => {
            // Fallback: semua field string non-kosong (skip meta/id).
            if let Some(obj) = payload.as_object() {
                for (k, v) in obj {
                    let kl = k.to_ascii_lowercase();
                    if kl.contains("company_id")
                        || kl.contains("company_symbol")
                        || kl.ends_with("_id")
                        || kl.contains("datahash")
                        || kl.contains("icon_url")
                        || kl == "corp_action_active"
                        || kl.contains("lock")
                    {
                        continue;
                    }
                    let s = match v {
                        Value::String(s) => s.trim().to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    if s.is_empty() || s == "0" {
                        continue;
                    }
                    push_corp_kv(&mut details, k, &s);
                }
            }
        }
    }

    if details.is_empty() {
        return None;
    }
    let mut group = HashMap::new();
    group.insert(label, details);
    Some(group)
}

fn parse_corpaction_json(v: &Value) -> CorporateAction {
    let Some(arr) = v.get("data").and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    arr.iter().filter_map(parse_corpaction_item).collect()
}

async fn fetch_corpaction(
    http: &reqwest::Client,
    bearer: &str,
    code: &str,
) -> Result<CorporateAction, Box<dyn std::error::Error>> {
    let url = format!("{CORPACTION_URL}/{code}?limit={CORPACTION_LIMIT}");
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, &format!("corpaction {code}"));
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("corpaction {code} HTTP {status}: {preview}").into());
    }

    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("corpaction {code} JSON: {e}"))?;
    Ok(parse_corpaction_json(&v))
}

fn parse_company_profile_json(v: &Value) -> CompanyProfile {
    let data = v.get("data").cloned().unwrap_or(Value::Null);

    let company_background = data
        .get("background")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    // API profile tidak selalu punya sector indeks; biarkan kosong.
    let sector = String::new();

    let mut shareholder_more_than_one_percent = Vec::new();
    if let Some(arr) = data
        .pointer("/shareholder_one_percent/shareholder")
        .and_then(|x| x.as_array())
    {
        for sh in arr {
            let name = sh
                .get("name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if name.is_empty() {
                continue;
            }
            let type_ = sh
                .get("classification")
                .and_then(|x| x.as_str())
                .filter(|s| !s.trim().is_empty())
                .or_else(|| sh.get("type").and_then(|x| x.as_str()))
                .unwrap_or("")
                .trim()
                .to_string();
            shareholder_more_than_one_percent.push(EmitenShareholderGt1 {
                name,
                type_,
                location: sh
                    .get("location")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                domicile: sh
                    .get("domicile")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                scriples: sh
                    .get("scripless")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                scrip: sh
                    .get("scrip")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                total_shares: sh
                    .get("value")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                percentage: sh
                    .get("percentage")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
            });
        }
    }

    let mut shareholders = Vec::new();
    if let Some(arr) = data.get("shareholder").and_then(|x| x.as_array()) {
        for sh in arr {
            let name = sh
                .get("name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if name.is_empty() {
                continue;
            }
            shareholders.push(EmitenShareholder {
                name,
                value: sh
                    .get("value")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                shares: sh
                    .get("percentage")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string(),
            });
        }
    }

    let ultimate_beneficial_owner = data
        .get("beneficiary")
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|b| b.get("name").and_then(|x| x.as_str()))
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    CompanyProfile {
        company_background,
        sector,
        shareholder_more_than_one_percent,
        shareholders,
        ultimate_beneficial_owner,
    }
}

async fn fetch_company_profile(
    http: &reqwest::Client,
    bearer: &str,
    code: &str,
) -> Result<CompanyProfile, Box<dyn std::error::Error>> {
    let url = format!("{PROFILE_URL}/{code}/profile");
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, &format!("emitten profile {code}"));
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("emitten profile {code} HTTP {status}: {preview}").into());
    }

    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("emitten profile {code} JSON: {e}"))?;
    let profile = parse_company_profile_json(&v);
    if profile.company_background.trim().is_empty() {
        return Err(format!("Profile {code}: Company Background kosong").into());
    }
    Ok(profile)
}

#[derive(Debug, DeserializeRow)]
struct EmitenLongNameRow {
    #[scylla(default_when_null)]
    long_name: String,
}

/// `long_name`: Redis (TTL 1 tahun) → search API (lalu cache) → DB `emiten_list` → kode.
async fn resolve_long_name(
    http: &reqwest::Client,
    bearer: &str,
    session: &Session,
    keyspace: &str,
    code: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(cached) = crate::redis_long_name::get_long_name(code).await {
        println!("long_name {code}: {cached} (redis)");
        return Ok(cached);
    }

    let api_name = match fetch_long_name_from_search(http, bearer, code).await {
        Ok(name) if !name.is_empty() => Some(name),
        Ok(_) => {
            eprintln!("Peringatan: search API {code} tidak menemukan desc — coba DB/fallback");
            None
        }
        Err(e) => {
            eprintln!("Peringatan: search API long_name {code}: {e}");
            None
        }
    };
    if let Some(name) = api_name {
        println!("long_name {code}: {name} (search API)");
        crate::redis_long_name::set_long_name(code, &name).await;
        return Ok(name);
    }

    let stmt = session
        .prepare(format!(
            "SELECT long_name FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (code,))
        .await?
        .into_rows_result()?;
    let existing = result
        .maybe_first_row::<EmitenLongNameRow>()?
        .map(|r| r.long_name.trim().to_string())
        .unwrap_or_default();
    if !existing.is_empty() && existing.to_ascii_uppercase() != code {
        return Ok(existing);
    }
    Ok(code.to_string())
}

fn parse_long_name_from_search(v: &Value, code: &str) -> Option<String> {
    let code_u = code.trim().to_ascii_uppercase();
    let companies = v.pointer("/data/company")?.as_array()?;
    // Prefer exact ticker match (type Saham bila ada), skip waran dll.
    let exact = companies.iter().find(|c| {
        let name = c
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        name == code_u
    });
    let pick = exact.or_else(|| {
        companies.iter().find(|c| {
            let sym2 = c
                .get("symbol_2")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();
            let sym3 = c
                .get("symbol_3")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();
            sym2 == code_u || sym3 == code_u
        })
    })?;
    let desc = pick
        .get("desc")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim();
    if desc.is_empty() {
        None
    } else {
        Some(desc.to_string())
    }
}

async fn fetch_long_name_from_search(
    http: &reqwest::Client,
    bearer: &str,
    code: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let keyword = code.trim().to_ascii_lowercase();
    let url = format!("{SEARCH_URL}?keyword={keyword}&page=0&type=all");
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, &format!("search {code}"));
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("search {code} HTTP {status}: {preview}").into());
    }

    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("search {code} JSON: {e}"))?;
    Ok(parse_long_name_from_search(&v, code).unwrap_or_default())
}

/// Upsert kolom hasil scrape saja.
/// Tidak mengisi / mengubah: `sector`, `is_konglomerasi`, `is_fundamental_solid`,
/// `is_blue_chip`, `is_plan_to_trade`, `catatan`, `catatan_owner`, `foto_owner`
/// (manual / aplikasi lain).
async fn upsert_emiten_list(
    session: &Session,
    keyspace: &str,
    code_name: &str,
    long_name: &str,
    emiten_icon: &str,
    key_stats: &HashMap<String, String>,
    net_income: &NetIncome,
    corporate_action: &CorporateAction,
    company_profile: &CompanyProfile,
) -> Result<(), Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_list (\
                code_name, long_name, emiten_icon, key_stats, net_income, \
                corporate_action, company_profile\
            ) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (
                code_name,
                long_name,
                emiten_icon,
                key_stats,
                net_income,
                corporate_action,
                company_profile,
            ),
        )
        .await?;

    touch_emiten_list_update_at(session, keyspace, code_name).await
}

async fn touch_emiten_list_update_at(
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let update = session
        .prepare(format!(
            "UPDATE {keyspace}.emiten_list SET update_at = toTimestamp(now()) WHERE code_name = ?"
        ))
        .await?;
    session.execute_unpaged(&update, (code_name,)).await?;
    Ok(())
}

/// `true` bila `update_at` masih < 30 hari dan `emiten_icon` sudah terisi (skip scrape/insert).
async fn is_update_at_fresh(
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let q = session
        .prepare(format!(
            "SELECT update_at, emiten_icon FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&q, (code_name,))
        .await?
        .into_rows_result()?;

    let Some(EmitenUpdateAtRow {
        update_at: Some(ts),
        emiten_icon,
    }) = result.maybe_first_row::<EmitenUpdateAtRow>()?
    else {
        return Ok(false);
    };

    Ok(!is_emiten_update_at_stale(Some(ts)) && !emiten_icon.trim().is_empty())
}

fn gcs_upload_ctx() -> Result<(&'static GcsSignedUrlRuntime, &'static GcsOAuthTokenCache), String> {
    static RUNTIME: OnceLock<Result<GcsSignedUrlRuntime, String>> = OnceLock::new();
    static OAUTH: OnceLock<GcsOAuthTokenCache> = OnceLock::new();
    let runtime = match RUNTIME.get_or_init(gcs::load_gcs_signed_url_runtime) {
        Ok(r) => r,
        Err(e) => return Err(e.clone()),
    };
    let oauth = OAUTH.get_or_init(GcsOAuthTokenCache::new);
    Ok((runtime, oauth))
}

/// URL icon Stockbit: `https://assets.stockbit.com/logos/companies/{CODE}.png`.
fn stockbit_emiten_icon_url(code: &str) -> String {
    format!(
        "{EMITEN_ICON_ASSETS_BASE}/{}.png",
        code.trim().to_ascii_uppercase()
    )
}

async fn upload_emiten_icon_to_gcs(
    emiten: &str,
    url: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if url.trim().is_empty() {
        return Ok(String::new());
    }
    let (runtime, oauth) = gcs_upload_ctx()?;
    download_and_upload_emiten_icon(emiten, url, runtime, oauth).await
}

/// Icon dari URL assets Stockbit → upload GCS. Reuse path GCS bila sudah ada di `emiten_list`.
async fn resolve_emiten_icon(
    session: &Session,
    keyspace: &str,
    code: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let existing = session
        .prepare(format!(
            "SELECT emiten_icon FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await;
    if let Ok(stmt) = existing {
        if let Ok(result) = session.execute_unpaged(&stmt, (code,)).await {
            if let Ok(rows) = result.into_rows_result() {
                if let Ok(Some(row)) = rows.maybe_first_row::<EmitenIconRow>() {
                    let path = row.emiten_icon.trim().to_string();
                    if !path.is_empty() {
                        return Ok(path);
                    }
                }
            }
        }
    }

    let url = stockbit_emiten_icon_url(code);
    match upload_emiten_icon_to_gcs(code, &url).await {
        Ok(path) => {
            if !path.is_empty() {
                println!("Key Stats: icon {code} dari {url} → GCS {path}");
            }
            Ok(path)
        }
        Err(e) => {
            eprintln!("Peringatan: GCS icon {code} ({url}): {e}");
            Ok(String::new())
        }
    }
}

/// Returns `Ok(true)` bila diinsert, `Ok(false)` bila di-skip (update_at masih < 30 hari).
async fn scrape_one_emiten(
    _page: &Page,
    http: &reqwest::Client,
    bearer: &str,
    session: &Session,
    keyspace: &str,
    emiten: &str,
    index: usize,
    total: usize,
) -> Result<bool, Box<dyn std::error::Error>> {
    scrape_one_emiten_inner(
        http, bearer, session, keyspace, emiten, index, total, true,
    )
    .await
}

/// Key Stats API + Corp. Action + Profile untuk satu `code_name` (tanpa skip fresh).
pub async fn scrape_emiten_list_for_code(
    page: &Page,
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<(), String> {
    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| e.to_string())?;
    let http = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())?;
    scrape_one_emiten_inner(
        &http, &bearer, session, keyspace, code_name, 1, 1, false,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

async fn scrape_one_emiten_inner(
    http: &reqwest::Client,
    bearer: &str,
    session: &Session,
    keyspace: &str,
    emiten: &str,
    index: usize,
    total: usize,
    skip_if_fresh: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let code = emiten.trim().to_ascii_uppercase();
    let progress = format!("{index}/{total}");

    if skip_if_fresh && is_update_at_fresh(session, keyspace, &code).await? {
        println!(
            "Key Stats: skip {code} ({progress}) — update_at belum melebihi {UPDATE_AT_FRESH_DAYS} hari ({})",
            format_elapsed(started)
        );
        return Ok(false);
    }

    println!("\nKey Stats API: {code} ({progress})...");
    let KeyStatsApiResult {
        key_stats,
        net_income,
    } = fetch_keystats_ratio(http, bearer, &code).await?;
    println!(
        "Key Stats API {code}: {} fields, {} net_income years",
        key_stats.len(),
        net_income.len()
    );

    if key_stats.is_empty() {
        return Err(format!("Key Stats kosong untuk {code}").into());
    }
    if !has_profitability_margins(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Profitability \
             (Gross/Operating/Net Profit Margin Quarter)"
        )
        .into());
    }
    if !has_market_cap_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Market Cap / Enterprise Value / \
             Current Share Outstanding / Free Float"
        )
        .into());
    }
    if !has_solvency_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Solvency \
             (Current/Quick/Debt ratios + Interest Coverage)"
        )
        .into());
    }
    if !has_cash_flow_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Cash Flow Statement \
             (Cash From Operations/Investing/Financing, CapEx, Free cash flow TTM)"
        )
        .into());
    }

    println!("Corp. Action API: {code}...");
    let corporate_action = fetch_corpaction(http, bearer, &code).await?;
    println!("Corp. Action {code}: {} items", corporate_action.len());

    println!("Profile API: {code}...");
    let company_profile = fetch_company_profile(http, bearer, &code).await?;
    println!(
        "Profile {code}: background_len={} gt1={} shareholders={} ubo={}",
        company_profile.company_background.len(),
        company_profile.shareholder_more_than_one_percent.len(),
        company_profile.shareholders.len(),
        company_profile.ultimate_beneficial_owner
    );

    let emiten_icon = resolve_emiten_icon(session, keyspace, &code).await?;
    let long_name = resolve_long_name(http, bearer, session, keyspace, &code).await?;

    upsert_emiten_list(
        session,
        keyspace,
        &code,
        &long_name,
        &emiten_icon,
        &key_stats,
        &net_income,
        &corporate_action,
        &company_profile,
    )
    .await?;

    println!(
        "OK: emiten_list {code} ({progress}) — {} key_stats, {} net_income years, \
         {} corporate_action, profile gt1={} shareholders={}{} ({})",
        key_stats.len(),
        net_income.len(),
        corporate_action.len(),
        company_profile.shareholder_more_than_one_percent.len(),
        company_profile.shareholders.len(),
        if emiten_icon.is_empty() {
            String::new()
        } else {
            format!(", icon={emiten_icon}")
        },
        format_elapsed(started)
    );
    Ok(true)
}

/// Key Stats API → Corp. Action + Profile → upsert `emiten_list`.
/// Upsert bila baris belum ada, atau `update_at` kosong / usia ≥ 30 hari;
/// skip bila `update_at` masih < 30 hari (dan `emiten_icon` sudah terisi).
pub async fn scrape_and_insert_key_stats(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emitens: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if emitens.is_empty() {
        println!("Tidak ada emiten untuk emiten_list key_stats.");
        return Ok(0);
    }

    println!("Key Stats API: ambil Bearer dari sesi browser...");
    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    println!("Bearer OK (len={}).", bearer.len());
    let http = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .timeout(Duration::from_secs(60))
        .build()?;

    let mut ok = 0usize;
    let total = emitens.len();
    for (i, emiten) in emitens.iter().enumerate() {
        let index = i + 1;
        match scrape_one_emiten(
            page, &http, &bearer, session, keyspace, emiten, index, total,
        )
        .await
        {
            Ok(true) => ok += 1,
            Ok(false) => {}
            Err(e) => {
                eprintln!("Peringatan: emiten_list {emiten} ({index}/{total}) gagal: {e}");
            }
        }
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_keystats_ratio_maps_stats_and_net_income() {
        let v: Value = serde_json::from_str(
            r#"{
            "data": {
                "closure_fin_items_results": [
                    {
                        "keystats_name": "Profitability",
                        "fin_name_results": [
                            {"fitem": {"name": "Gross Profit Margin (Quarter)", "value": "35.63%"}},
                            {"fitem": {"name": "Operating Profit Margin (Quarter)", "value": "16.85%"}},
                            {"fitem": {"name": "Net Profit Margin (Quarter)", "value": "11.87%"}}
                        ]
                    },
                    {
                        "keystats_name": "Solvency",
                        "fin_name_results": [
                            {"fitem": {"name": "Current Ratio (Quarter)", "value": "2.26"}},
                            {"fitem": {"name": "Quick Ratio (Quarter)", "value": "2.11"}},
                            {"fitem": {"name": "Debt to Equity Ratio (Quarter)", "value": "0.85"}},
                            {"fitem": {"name": "LT Debt/Equity (Quarter)", "value": "0.71"}},
                            {"fitem": {"name": "Total Liabilities/Equity (Quarter)", "value": "1.18"}},
                            {"fitem": {"name": "Total Debt/Total Assets (Quarter)", "value": "0.35"}},
                            {"fitem": {"name": "Interest Coverage (TTM)", "value": "4.82"}}
                        ]
                    },
                    {
                        "keystats_name": "Cash Flow Statement",
                        "fin_name_results": [
                            {"fitem": {"name": "Cash From Operations (TTM)", "value": "3,294 B"}},
                            {"fitem": {"name": "Cash From Investing (TTM)", "value": "(16,220 B)"}},
                            {"fitem": {"name": "Cash From Financing (TTM)", "value": "5,253 B"}},
                            {"fitem": {"name": "Capital expenditure (TTM)", "value": "(6,965 B)"}},
                            {"fitem": {"name": "Free cash flow (TTM)", "value": "(3,671 B)"}}
                        ]
                    }
                ],
                "stats": {
                    "current_share_outstanding": "192.64 B",
                    "market_cap": "154,110 B",
                    "enterprise_value": "181,042 B",
                    "free_float": "19.35%"
                },
                "financial_year_parent": {
                    "financial_year_groups": [
                        {
                            "fitem_name": "Net Income",
                            "most_recent_quarter": {"quarter": "Q1"},
                            "financial_year_values": [
                                {
                                    "year": "2026",
                                    "period_values": [
                                        {"period": "Q1", "quarter_value": "1,387 B"}
                                    ],
                                    "annualised_value": "5,547 B",
                                    "ttm_value": "3,855 B"
                                }
                            ]
                        }
                    ]
                }
            }
        }"#,
        )
        .unwrap();
        let parsed = parse_keystats_ratio_json(&v);
        assert!(has_profitability_margins(&parsed.key_stats));
        assert!(has_market_cap_block(&parsed.key_stats));
        assert!(has_solvency_block(&parsed.key_stats));
        assert!(has_cash_flow_block(&parsed.key_stats));
        assert_eq!(
            parsed.key_stats.get("Market Cap").map(String::as_str),
            Some("154,110 B")
        );
        assert_eq!(
            parsed
                .net_income
                .get("2026")
                .and_then(|y| y.get("TTM (Q1)"))
                .map(String::as_str),
            Some("3,855 B")
        );
        assert_eq!(
            parsed
                .net_income
                .get("2026")
                .and_then(|y| y.get("Q1"))
                .map(String::as_str),
            Some("1,387 B")
        );
    }

    #[test]
    fn key_stats_json_parses() {
        let json = r#"{"Current PE Ratio (TTM)":"74.08","Revenue (Quarter YoY Growth)":"-65.36%"}"#;
        let map: HashMap<String, String> = serde_json::from_str(json).unwrap();
        assert_eq!(map.get("Current PE Ratio (TTM)").map(String::as_str), Some("74.08"));
    }

    #[test]
    fn net_income_json_parses_by_year() {
        let json = r#"{
            "2026": {
                "Q1": "1,387 B",
                "Q2": "-",
                "Q3": "-",
                "Q4": "-",
                "Annualised": "5,547 B",
                "TTM (Q1)": "3,855 B"
            },
            "2025": {
                "Q1": "1,317 B",
                "Q2": "274 B",
                "Q3": "1,312 B",
                "Q4": "888 B",
                "Annualised": "3,799 B",
                "TTM (Q1)": "3,799 B"
            }
        }"#;
        let map: NetIncome = serde_json::from_str(json).unwrap();
        assert_eq!(
            map.get("2026").and_then(|y| y.get("Q1")).map(String::as_str),
            Some("1,387 B")
        );
        assert_eq!(
            map.get("2025")
                .and_then(|y| y.get("Annualised"))
                .map(String::as_str),
            Some("3,799 B")
        );
    }

    #[test]
    fn profitability_margins_detected() {
        let mut map = HashMap::new();
        map.insert("Gross Profit Margin (Quarter)".into(), "35.63%".into());
        map.insert("Operating Profit Margin (Quarter)".into(), "16.85%".into());
        map.insert("Net Profit Margin (Quarter)".into(), "11.87%".into());
        assert!(has_profitability_margins(&map));
    }

    #[test]
    fn market_cap_block_detected() {
        let mut map = HashMap::new();
        map.insert("Market Cap".into(), "155,074 B".into());
        map.insert("Enterprise Value".into(), "182,005 B".into());
        map.insert("Current Share Outstanding".into(), "192.64 B".into());
        map.insert("Free Float".into(), "19.35%".into());
        assert!(has_market_cap_block(&map));
    }

    #[test]
    fn solvency_block_detected() {
        let mut map = HashMap::new();
        map.insert("Current Ratio (Quarter)".into(), "2.26".into());
        map.insert("Quick Ratio (Quarter)".into(), "2.11".into());
        map.insert("Debt to Equity Ratio (Quarter)".into(), "0.85".into());
        map.insert("LT Debt/Equity (Quarter)".into(), "0.71".into());
        map.insert("Total Liabilities/Equity (Quarter)".into(), "1.18".into());
        map.insert("Total Debt/Total Assets (Quarter)".into(), "0.35".into());
        map.insert("Interest Coverage (TTM)".into(), "4.82".into());
        assert!(has_solvency_block(&map));
    }

    #[test]
    fn cash_flow_block_detected() {
        let mut map = HashMap::new();
        map.insert("Cash From Operations (TTM)".into(), "3,294 B".into());
        map.insert("Cash From Investing (TTM)".into(), "(16,220 B)".into());
        map.insert("Cash From Financing (TTM)".into(), "5,253 B".into());
        map.insert("Capital expenditure (TTM)".into(), "(6,965 B)".into());
        map.insert("Free cash flow (TTM)".into(), "(3,671 B)".into());
        assert!(has_cash_flow_block(&map));
    }

    #[test]
    fn parse_corpaction_api_rups_and_stocksplit() {
        let v: Value = serde_json::from_str(
            r#"{
            "data": [
                {
                    "action_type": "rups",
                    "action_info": {
                        "rups": {
                            "rups_date": "2026-06-09",
                            "rups_time": "14:00",
                            "rups_eligible_date": "2026-05-06",
                            "rups_venue": "Jakarta\nPusat"
                        }
                    }
                },
                {
                    "action_type": "stocksplit",
                    "action_info": {
                        "stocksplit": {
                            "stocksplit_cumdate": "2026-04-08",
                            "stocksplit_exdate": "2026-04-09",
                            "stocksplit_recdate": "2026-04-10",
                            "stocksplit_old": "1",
                            "stocksplit_new": "25",
                            "stocksplit_new_price": 2680
                        }
                    }
                }
            ],
            "message": "ok"
        }"#,
        )
        .unwrap();
        let items = parse_corpaction_json(&v);
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]["RUPS"]
                .iter()
                .find_map(|m| m.get("Event Date"))
                .map(String::as_str),
            Some("2026-06-09")
        );
        assert_eq!(
            items[0]["RUPS"]
                .iter()
                .find_map(|m| m.get("Venue"))
                .map(String::as_str),
            Some("Jakarta Pusat")
        );
        assert_eq!(
            items[1]["Stock Split"]
                .iter()
                .find_map(|m| m.get("Ratio"))
                .map(String::as_str),
            Some("1:25")
        );
        assert_eq!(
            items[1]["Stock Split"]
                .iter()
                .find_map(|m| m.get("New Price"))
                .map(String::as_str),
            Some("2680")
        );
    }

    #[test]
    fn corporate_action_json_parses() {
        let json = r#"[
          {
            "Dividend": [
              {"Dividend": "Rp 209"},
              {"Cum Date": "20 Apr 26"},
              {"Ex Date": "21 Apr 26"},
              {"Recording Date": "22 Apr 26"},
              {"Payment Date": "8 Mei 26"}
            ]
          },
          {
            "RUPS": [
              {"Event Date": "10 Apr 26"},
              {"Time": "14:00"},
              {"Eligible Date": "10 Mar 26"},
              {"Venue": "Jakarta"}
            ]
          }
        ]"#;
        let items: CorporateAction = serde_json::from_str(json).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]["Dividend"][0].get("Dividend").map(String::as_str),
            Some("Rp 209")
        );
        assert_eq!(
            items[1]["RUPS"][0].get("Event Date").map(String::as_str),
            Some("10 Apr 26")
        );
    }

    #[test]
    fn parse_long_name_from_search_picks_exact_ticker_desc() {
        let v: Value = serde_json::from_str(
            r#"{
            "data": {
                "company": [
                    {
                        "name": "DSSA",
                        "desc": "Dian Swastatika Sentosa Tbk",
                        "type": "Saham",
                        "symbol_2": "DSSA"
                    },
                    {
                        "name": "DSSAZPCZ6A",
                        "desc": "Call Waran DSSA ZP",
                        "type": "Waran",
                        "symbol_2": "DSSAZPCZ6A"
                    }
                ]
            }
        }"#,
        )
        .unwrap();
        assert_eq!(
            parse_long_name_from_search(&v, "DSSA").as_deref(),
            Some("Dian Swastatika Sentosa Tbk")
        );
        assert_eq!(
            parse_long_name_from_search(&v, "dssa").as_deref(),
            Some("Dian Swastatika Sentosa Tbk")
        );
    }

    #[test]
    fn parse_company_profile_api() {
        let v: Value = serde_json::from_str(
            r#"{
            "data": {
                "background": "PT Dian Swastatika Sentosa Tbk menjalankan kegiatan usaha utama.",
                "shareholder": [
                    {"name": "PT SINAR MAS TUNGGAL", "value": "115.39 B", "percentage": "59.9%"}
                ],
                "beneficiary": [{"name": "FRANKY OESMAN WIDJAJA"}],
                "shareholder_one_percent": {
                    "shareholder": [
                        {
                            "name": "SINAR MAS TUNGGAL",
                            "classification": "Corporate",
                            "location": "Local",
                            "domicile": "INDONESIA",
                            "scripless": "0",
                            "scrip": "115,388,080,000",
                            "value": "115,388,080,000",
                            "percentage": "59.90%"
                        }
                    ]
                }
            }
        }"#,
        )
        .unwrap();
        let p = parse_company_profile_json(&v);
        assert!(p.company_background.contains("Dian Swastatika"));
        assert_eq!(p.shareholders.len(), 1);
        assert_eq!(p.shareholders[0].shares, "59.9%");
        assert_eq!(p.shareholder_more_than_one_percent.len(), 1);
        assert_eq!(
            p.shareholder_more_than_one_percent[0].type_,
            "Corporate"
        );
        assert_eq!(p.shareholder_more_than_one_percent[0].scriples, "0");
        assert_eq!(p.ultimate_beneficial_owner, "FRANKY OESMAN WIDJAJA");
    }

    #[test]
    fn company_profile_json_parses() {
        let json = r#"{
          "company_background": "PT Dian Swastatika Sentosa Tbk menjalankan kegiatan usaha utama.",
          "sector": "Minyak, Gas & Batu Bara",
          "shareholder_more_than_one_percent": [
            {
              "name": "SINAR MAS TUNGGAL",
              "type": "Corporate",
              "location": "Local",
              "domicile": "INDONESIA",
              "scriples": "0",
              "scrip": "115,388,080,000",
              "total_shares": "115,388,080,000",
              "percentage": "59.90%"
            }
          ],
          "shareholders": [
            {
              "name": "PT SINAR MAS TUNGGAL",
              "value": "115.39 B",
              "shares": "59.9%"
            }
          ],
          "ultimate_beneficial_owner": "FRANKY OESMAN WIDJAJA"
        }"#;
        let profile: CompanyProfile = serde_json::from_str(json).unwrap();
        assert!(profile.company_background.contains("Dian Swastatika"));
        assert_eq!(profile.sector, "Minyak, Gas & Batu Bara");
        assert_eq!(profile.shareholder_more_than_one_percent.len(), 1);
        assert_eq!(
            profile.shareholder_more_than_one_percent[0].type_,
            "Corporate"
        );
        assert_eq!(profile.shareholders[0].name, "PT SINAR MAS TUNGGAL");
        assert_eq!(
            profile.ultimate_beneficial_owner,
            "FRANKY OESMAN WIDJAJA"
        );
    }
}
