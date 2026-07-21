//! Portfolio via API `carina.stockbit.com/portfolio/v2/list` → upsert `portofolio`.
//!
//! Alur: bila tombol START TRADING masih ada → klik → input PIN (`STOCKBUT_PIN` /
//! `STOCKBIT_PIN`) → Submit → tunggu modal hilang.
//! Lalu DOM scrape header equity ([`portofolio_equity_worker`]) ke `portofolio_equity`
//! **sebelum** GET portfolio API.
//! Setelah itu ambil **Bearer trading** (pasca-PIN / `securitiesAccessToken`),
//! **bukan** Bearer login web Exodus, lalu GET portfolio API.
//!
//! Sebelum upsert `portofolio`: pastikan `emiten_list` + `bandarmology` terisi
//! (scrape bila belum ada). Icon: reuse `emiten_list.emiten_icon` bila sudah ada;
//! download GCS hanya bila belum ada.
//! Setelah upsert `portofolio`: salin minggu berjalan bandarmology →
//! `portofolio_bandarmology` (per `emiten_name`, sama RPC `InsertPortofolioBandarmology`),
//! lalu hapus orphan yang tidak ada di `portofolio` (sama RPC `DeletePortofolioBandarmology`).

use chrono::{DateTime, Local, Utc};
use chromiumoxide::page::Page;
use gcs::{download_and_upload_emiten_icon, GcsOAuthTokenCache, GcsSignedUrlRuntime};
use rand::Rng;
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use stockbit_browser::goto_stockbit;
use tokio::time::sleep;

use crate::{
    bandarmology_worker, emiten_list_worker, portofolio_bandarmology_worker,
    portofolio_equity_worker,
};

const PORTFOLIO_API_URL: &str = "https://carina.stockbit.com/portfolio/v2/list";
const ORDER_LIST_API_URL: &str = "https://carina.stockbit.com/order/v2/list";
const STOCKBIT_PORTFOLIO_URL: &str = "https://stockbit.com/securities/portfolio";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// UDT `portofolio_history_item` untuk INSERT/UPDATE map history.
#[derive(Debug, Clone, SerializeValue)]
pub struct PortofolioHistoryItemUd {
    pub order_id: String,
    pub message: String,
    pub symbol: String,
    pub side: String,
    pub lot_done: i32,
    pub price_average: f64,
    pub amount_matched: f64,
}

#[derive(Debug, Clone)]
struct PortoRow {
    emiten_name: String,
    /// Dari API `company.name` / `company.company_name` (fallback).
    long_name: String,
    emiten_icon_url: String,
    balance_lot: i64,
    available_lot: i64,
    average_price: f64,
    current_price: f64,
    invested: f64,
    market_value: f64,
    potential_p_l: f64,
    /// Persentase (gain API × 100), contoh `-1.9401`.
    percentage: f64,
}

fn trading_pin() -> Result<String, Box<dyn std::error::Error>> {
    let pin = std::env::var("STOCKBUT_PIN")
        .or_else(|_| std::env::var("STOCKBIT_PIN"))
        .unwrap_or_default();
    let pin = pin.trim().to_string();
    if pin.is_empty() {
        return Err("STOCKBUT_PIN (atau STOCKBIT_PIN) wajib diisi di .env".into());
    }
    if !pin.chars().all(|c| c.is_ascii_digit()) {
        return Err("STOCKBUT_PIN harus berupa angka".into());
    }
    Ok(pin)
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

#[derive(Debug, DeserializeRow)]
struct EmitenIconRow {
    #[scylla(default_when_null)]
    emiten_icon: String,
}

#[derive(Debug, DeserializeRow)]
struct EmitenLongNameRow {
    #[scylla(default_when_null)]
    long_name: String,
}

/// Path GCS dari `emiten_list.emiten_icon` bila sudah terisi.
async fn emiten_list_icon(
    session: &Session,
    keyspace: &str,
    code: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let stmt = session
        .prepare(format!(
            "SELECT emiten_icon FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (code,))
        .await?
        .into_rows_result()?;
    if let Some(row) = result.maybe_first_row::<EmitenIconRow>()? {
        let path = row.emiten_icon.trim().to_string();
        if !path.is_empty() {
            return Ok(path);
        }
    }
    Ok(String::new())
}

/// `long_name` dari `emiten_list` bila sudah terisi.
async fn emiten_list_long_name(
    session: &Session,
    keyspace: &str,
    code: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let stmt = session
        .prepare(format!(
            "SELECT long_name FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (code,))
        .await?
        .into_rows_result()?;
    if let Some(row) = result.maybe_first_row::<EmitenLongNameRow>()? {
        let name = row.long_name.trim().to_string();
        if !name.is_empty() {
            return Ok(name);
        }
    }
    Ok(String::new())
}

/// Prefer Redis → `emiten_list.long_name` → nama dari API portfolio (lalu cache Redis).
async fn resolve_portofolio_long_name(
    session: &Session,
    keyspace: &str,
    emiten: &str,
    api_long_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(cached) = crate::redis_long_name::get_long_name(emiten).await {
        return Ok(cached);
    }

    let from_list = emiten_list_long_name(session, keyspace, emiten).await?;
    if !from_list.is_empty() {
        return Ok(from_list);
    }

    let from_api = api_long_name.trim().to_string();
    if !from_api.is_empty() {
        crate::redis_long_name::set_long_name(emiten, &from_api).await;
    }
    Ok(from_api)
}

/// Reuse icon dari `emiten_list`; download ke GCS hanya bila belum ada.
async fn resolve_portofolio_icon(
    session: &Session,
    keyspace: &str,
    emiten: &str,
    icon_url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let existing = emiten_list_icon(session, keyspace, emiten).await?;
    if !existing.is_empty() {
        return Ok(existing);
    }
    match upload_emiten_icon_to_gcs(emiten, icon_url).await {
        Ok(path) => {
            if !path.is_empty() {
                println!("Portofolio: icon {emiten} di-upload GCS (belum ada di emiten_list) → {path}");
            }
            Ok(path)
        }
        Err(e) => {
            eprintln!("Peringatan: gagal upload icon GCS {emiten}: {e}");
            Ok(String::new())
        }
    }
}

async fn is_start_trading_visible(page: &Page) -> Result<bool, Box<dyn std::error::Error>> {
    let visible = page
        .evaluate(
            r#"(() => {
                const btn = document.querySelector('[data-cy="top-navbar-button-start-trading"]');
                if (btn) {
                    const style = window.getComputedStyle(btn);
                    if (style.display === 'none' || style.visibility === 'hidden') return false;
                    return true;
                }
                const nodes = Array.from(document.querySelectorAll('p, button, div, a'));
                return nodes.some((el) => {
                    const t = (el.innerText || el.textContent || '').trim();
                    return t === 'START TRADING' || t.includes('START TRADING');
                });
            })()"#,
        )
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    Ok(visible)
}

async fn click_start_trading(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=20 {
        let clicked = page
            .evaluate(
                r#"(() => {
                    const btn = document.querySelector('[data-cy="top-navbar-button-start-trading"]');
                    if (btn) { btn.click(); return true; }
                    const nodes = Array.from(document.querySelectorAll('p, button, div, a'));
                    const target = nodes.find((el) => {
                        const t = (el.innerText || el.textContent || '').trim();
                        return t === 'START TRADING' || t.includes('START TRADING');
                    });
                    if (!target) return false;
                    (target.closest('[data-cy="top-navbar-button-start-trading"]') || target).click();
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("START TRADING diklik (attempt {attempt}).");
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Tombol START TRADING tidak ditemukan".into())
}

async fn wait_for_pin_modal(page: &Page, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const modal = document.querySelector('.ant-modal-body');
                    if (!modal) return false;
                    const text = (modal.innerText || '').toLowerCase();
                    if (!text.includes('input trading pin') && !text.includes('trading pin')) {
                        return false;
                    }
                    const input = modal.querySelector('input[pattern="\\d*"], input.sc-916940df-2, input[type="text"]');
                    return !!input;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            println!("Modal Input Trading PIN muncul.");
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Timeout menunggu modal Input Trading PIN".into());
        }
        sleep(Duration::from_millis(300)).await;
    }
}

async fn type_pin_natural(page: &Page, pin: &str) -> Result<(), Box<dyn std::error::Error>> {
    let focused = page
        .evaluate(
            r#"(() => {
                const modal = document.querySelector('.ant-modal-body');
                if (!modal) return false;
                const input = modal.querySelector('input[pattern="\\d*"], input.sc-916940df-2, input[type="text"]');
                if (!input) return false;
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                return true;
            })()"#,
        )
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !focused {
        return Err("Input PIN tidak ditemukan".into());
    }

    let selector = r#".ant-modal-body input[pattern="\d*"], .ant-modal-body input.sc-916940df-2"#;
    let element = match page.find_element(selector).await {
        Ok(el) => Some(el),
        Err(_) => page
            .find_element(r#".ant-modal-body input[type="text"]"#)
            .await
            .ok(),
    };

    println!("Ketik Trading PIN secara natural (delay 50–300ms/karakter)...");
    for ch in pin.chars() {
        let delay = rand::thread_rng().gen_range(50u64..=300);
        sleep(Duration::from_millis(delay)).await;
        if let Some(ref el) = element {
            el.type_str(&ch.to_string()).await?;
        } else {
            let ch_js = serde_json::to_string(&ch.to_string()).unwrap_or_else(|_| "\"\"".into());
            let ok = page
                .evaluate(format!(
                    r#"(() => {{
                        const modal = document.querySelector('.ant-modal-body');
                        if (!modal) return false;
                        const input = modal.querySelector('input[pattern="\\d*"], input.sc-916940df-2, input[type="text"]');
                        if (!input) return false;
                        input.value = (input.value || '') + {ch_js};
                        input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        return true;
                    }})()"#
                ))
                .await?
                .into_value::<bool>()
                .unwrap_or(false);
            if !ok {
                return Err("Gagal mengetik karakter PIN".into());
            }
        }
    }
    Ok(())
}

async fn click_pin_submit(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=15 {
        let clicked = page
            .evaluate(
                r#"(() => {
                    const modal = document.querySelector('.ant-modal-body');
                    if (!modal) return false;
                    const buttons = Array.from(modal.querySelectorAll('button'));
                    const submit = buttons.find((b) => {
                        const t = (b.innerText || b.textContent || '').trim();
                        return t === 'Submit' || t.includes('Submit');
                    });
                    if (!submit) return false;
                    if (submit.disabled) {
                        const input = modal.querySelector('input[pattern="\\d*"], input.sc-916940df-2, input[type="text"]');
                        if (input && (input.value || '').length > 0) {
                            submit.disabled = false;
                            submit.removeAttribute('disabled');
                        }
                    }
                    if (submit.disabled) return false;
                    submit.click();
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("Tombol Submit PIN diklik (attempt {attempt}).");
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Tombol Submit PIN tidak bisa diklik (masih disabled / tidak ada)".into())
}

async fn wait_pin_modal_gone(page: &Page, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let gone = page
            .evaluate(
                r#"(() => {
                    const modal = document.querySelector('.ant-modal-body');
                    if (!modal) return true;
                    const text = (modal.innerText || '').toLowerCase();
                    return !(text.includes('input trading pin') || text.includes('trading pin'));
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if gone {
            println!("Modal Trading PIN hilang.");
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Timeout menunggu modal Trading PIN hilang".into());
        }
        sleep(Duration::from_millis(400)).await;
    }
}

/// Masuk mode trading: klik START TRADING + PIN bila perlu; lewati bila tombol sudah hilang.
pub async fn ensure_trading_session(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    if !is_start_trading_visible(page).await? {
        println!("START TRADING tidak terlihat — sudah mode trading, lewati PIN.");
        return Ok(());
    }

    println!("Portofolio: klik START TRADING...");
    click_start_trading(page).await?;
    let pin = trading_pin()?;
    wait_for_pin_modal(page, Duration::from_secs(30)).await?;
    type_pin_natural(page, &pin).await?;
    click_pin_submit(page).await?;
    wait_pin_modal_gone(page, Duration::from_secs(45)).await?;
    Ok(())
}

fn ensure_auth_capture_js() -> &'static str {
    r#"(() => {
        try {
            if (window.__sbCaptureAuthInstalled) {
                return (window.__sbCapturedBearer || '').length;
            }
            window.__sbCaptureAuthInstalled = true;
            window.__sbCapturedBearer = window.__sbCapturedBearer || '';
            const remember = (v) => {
                if (!v || typeof v !== 'string') return;
                const t = v.replace(/^Bearer\s+/i, '').trim();
                if (t.startsWith('eyJ')) window.__sbCapturedBearer = t;
            };
            const wrapHeaders = (headers) => {
                if (!headers) return;
                try {
                    if (typeof headers.get === 'function') {
                        remember(headers.get('Authorization') || headers.get('authorization'));
                    } else if (Array.isArray(headers)) {
                        for (const pair of headers) {
                            if (pair && String(pair[0]).toLowerCase() === 'authorization') remember(pair[1]);
                        }
                    } else if (typeof headers === 'object') {
                        remember(headers.Authorization || headers.authorization);
                    }
                } catch (_) {}
            };
            const ofetch = window.fetch;
            window.fetch = function (input, init) {
                try {
                    if (init && init.headers) wrapHeaders(init.headers);
                    if (input && typeof input === 'object' && input.headers) wrapHeaders(input.headers);
                } catch (_) {}
                return ofetch.apply(this, arguments);
            };
            const oSet = XMLHttpRequest.prototype.setRequestHeader;
            XMLHttpRequest.prototype.setRequestHeader = function (k, v) {
                try {
                    if (String(k).toLowerCase() === 'authorization') remember(v);
                } catch (_) {}
                return oSet.apply(this, arguments);
            };
            return (window.__sbCapturedBearer || '').length;
        } catch (_) { return 0; }
    })()"#
}

async fn probe_trading_bearer(http: &reqwest::Client, token: &str) -> Result<u16, String> {
    let resp = http
        .get(PORTFOLIO_API_URL)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp.status().as_u16())
}

/// Bearer trading pasca-PIN (`securitiesAccessToken` / Authorization ke carina).
/// Sengaja **tidak** memakai `extract_stockbit_bearer` (login Exodus).
pub async fn extract_trading_bearer_after_pin(
    page: &Page,
) -> Result<String, Box<dyn std::error::Error>> {
    let _ = page.evaluate(ensure_auth_capture_js()).await?;
    // Buang capture login web lama; nanti diisi ulang oleh SPA trading.
    let _ = page
        .evaluate(r#"(() => { window.__sbCapturedBearer = ''; return 0; })()"#)
        .await;

    println!("Portofolio: warm-up trading (buka {STOCKBIT_PORTFOLIO_URL}) agar Bearer carina tertangkap...");
    goto_stockbit(page, STOCKBIT_PORTFOLIO_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    let _ = page.evaluate(ensure_auth_capture_js()).await?;

    for _ in 0..25 {
        sleep(Duration::from_millis(400)).await;
        let n = page
            .evaluate(r#"(() => (window.__sbCapturedBearer || '').length)()"#)
            .await?
            .into_value::<u64>()
            .unwrap_or(0);
        if n > 0 {
            println!("Portofolio: network.capture trading siap (len={n}).");
            break;
        }
    }

    let scanned = page
        .evaluate(
            r#"(() => {
                const hits = [];
                const push = (token, source) => {
                    if (!token || typeof token !== 'string') return;
                    const t = token.replace(/^Bearer\s+/i, '').trim();
                    if (!t.startsWith('eyJ')) return;
                    hits.push({ token: t, source, len: t.length });
                };
                try { push(window.__sbCapturedBearer || '', 'network.capture'); } catch (_) {}
                try {
                    push(localStorage.getItem('securitiesAccessToken') || '', 'localStorage:securitiesAccessToken');
                    push(sessionStorage.getItem('securitiesAccessToken') || '', 'sessionStorage:securitiesAccessToken');
                } catch (_) {}
                // Dedup by token, keep first (capture preferred).
                const seen = new Set();
                const out = [];
                for (const h of hits) {
                    if (seen.has(h.token)) continue;
                    seen.add(h.token);
                    out.push(h);
                }
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let candidates: Vec<Value> = serde_json::from_str(&scanned).unwrap_or_default();
    {
        let summary: Vec<String> = candidates
            .iter()
            .map(|h| {
                format!(
                    "{} len={}",
                    h.get("source").and_then(|x| x.as_str()).unwrap_or("-"),
                    h.get("len").and_then(|x| x.as_u64()).unwrap_or(0),
                )
            })
            .collect();
        println!(
            "Portofolio trading bearer candidates ({}): {}",
            summary.len(),
            if summary.is_empty() {
                "(none)".into()
            } else {
                summary.join(" | ")
            }
        );
    }

    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(45))
        .build()?;

    let mut probe_log = Vec::new();
    for h in &candidates {
        let source = h.get("source").and_then(|x| x.as_str()).unwrap_or("-");
        let token = h.get("token").and_then(|x| x.as_str()).unwrap_or("");
        if !token.starts_with("eyJ") {
            continue;
        }
        match probe_trading_bearer(&http, token).await {
            Ok(status) => {
                probe_log.push(format!("{source} len={} status={status}", token.len()));
                if (200..300).contains(&status) {
                    println!("Portofolio trading bearer probe: {}", probe_log.join(" | "));
                    println!(
                        "Portofolio trading bearer dipilih: source={source} len={}",
                        token.len()
                    );
                    return Ok(token.to_string());
                }
            }
            Err(e) => probe_log.push(format!("{source} len={} err={e}", token.len())),
        }
    }
    if !probe_log.is_empty() {
        println!("Portofolio trading bearer probe: {}", probe_log.join(" | "));
    }
    Err(
        "Bearer trading untuk carina.stockbit.com/portfolio tidak ditemukan. \
         Pastikan PIN sukses dan sesi trading aktif."
            .into(),
    )
}

fn json_f64(v: &Value, path: &[&str]) -> f64 {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(x) => x,
            None => return 0.0,
        };
    }
    cur.as_f64()
        .or_else(|| cur.as_i64().map(|n| n as f64))
        .or_else(|| cur.as_u64().map(|n| n as f64))
        .unwrap_or(0.0)
}

fn json_i64(v: &Value, path: &[&str]) -> i64 {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(x) => x,
            None => return 0,
        };
    }
    cur.as_i64()
        .or_else(|| cur.as_u64().map(|n| n as i64))
        .or_else(|| cur.as_f64().map(|n| n as i64))
        .unwrap_or(0)
}

fn parse_portfolio_list_json(v: &Value) -> Vec<PortoRow> {
    let results = v
        .pointer("/data/results")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::with_capacity(results.len());
    for item in results {
        let symbol = item
            .get("symbol")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let icon = item
            .pointer("/company/icon_url")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let long_name = item
            .pointer("/company/name")
            .or_else(|| item.pointer("/company/company_name"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let gain = json_f64(&item, &["asset", "unrealised", "gain"]);
        rows.push(PortoRow {
            emiten_name: symbol,
            long_name,
            emiten_icon_url: icon,
            balance_lot: json_i64(&item, &["qty", "balance", "lot"]),
            available_lot: json_i64(&item, &["qty", "available", "lot"]),
            average_price: json_f64(&item, &["price", "average", "price"]),
            current_price: json_f64(&item, &["price", "latest"]),
            invested: json_f64(&item, &["asset", "amount_invested"]),
            market_value: json_f64(&item, &["asset", "unrealised", "market_value"]),
            potential_p_l: json_f64(&item, &["asset", "unrealised", "profit_loss"]),
            percentage: gain * 100.0,
        });
    }
    rows
}

async fn fetch_portfolio_list(
    http: &reqwest::Client,
    bearer: &str,
) -> Result<Vec<PortoRow>, Box<dyn std::error::Error>> {
    let resp = http
        .get(PORTFOLIO_API_URL)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, "portfolio/v2/list");
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("portfolio/v2/list HTTP {status}: {preview}").into());
    }

    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("portfolio/v2/list JSON: {e}"))?;
    let rows = parse_portfolio_list_json(&v);
    if rows.is_empty() {
        return Err("portfolio/v2/list: data.results kosong".into());
    }
    Ok(rows)
}

async fn upsert_portofolio(
    session: &Session,
    keyspace: &str,
    rows: &[PortoRow],
) -> Result<usize, Box<dyn std::error::Error>> {
    println!("Portofolio: TRUNCATE {keyspace}.portofolio...");
    session
        .query_unpaged(format!("TRUNCATE {keyspace}.portofolio"), &[])
        .await?;
    println!("Portofolio: truncate selesai — mulai insert {} baris...", rows.len());

    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio (\
                emiten_name, long_name, emiten_icon, balance_lot, available_lot, \
                average_price, current_price, invested, market_value, \
                potential_p_l, percentage\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let mut n = 0usize;
    for row in rows {
        let emiten = row.emiten_name.trim().to_ascii_uppercase();
        if emiten.is_empty() {
            continue;
        }
        let emiten_icon =
            resolve_portofolio_icon(session, keyspace, &emiten, &row.emiten_icon_url).await?;
        let long_name =
            resolve_portofolio_long_name(session, keyspace, &emiten, &row.long_name).await?;

        session
            .execute_unpaged(
                &insert,
                (
                    emiten.as_str(),
                    long_name.as_str(),
                    emiten_icon.as_str(),
                    row.balance_lot,
                    row.available_lot,
                    row.average_price,
                    row.current_price,
                    row.invested,
                    row.market_value,
                    row.potential_p_l,
                    row.percentage,
                ),
            )
            .await?;
        n += 1;
        println!(
            "Insert portofolio [{n}/{}]: {emiten} ({long_name}) \
             balance_lot={} available_lot={} avg={:.4} last={} \
             invested={:.2} mv={:.2} pl={:.2} pct={:.4}%{}",
            rows.len(),
            row.balance_lot,
            row.available_lot,
            row.average_price,
            row.current_price,
            row.invested,
            row.market_value,
            row.potential_p_l,
            row.percentage,
            if emiten_icon.is_empty() {
                String::new()
            } else {
                format!(" icon={emiten_icon}")
            },
        );
    }
    println!("Insert portofolio selesai: {n}/{} baris.", rows.len());
    Ok(n)
}

/// START TRADING (opsional) → PIN (opsional) → DOM `portofolio_equity` →
/// Bearer trading → portfolio API → pastikan `emiten_list` + `bandarmology` →
/// upsert `portofolio` → salin minggu berjalan ke `portofolio_bandarmology`.
pub async fn scrape_and_insert_portofolio(
    page: &Page,
    session: &Arc<Session>,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    ensure_trading_session(page).await?;

    println!("Jeda 2 detik setelah PIN / mode trading siap...");
    sleep(Duration::from_secs(2)).await;

    println!("Portofolio equity: DOM scrape header sebelum portfolio API...");
    let equity_ok = portofolio_equity_worker::scrape_and_insert_portofolio_equity(
        page,
        session.as_ref(),
        keyspace,
    )
    .await?;
    println!("OK: {equity_ok} baris diupsert ke portofolio_equity (sebelum portfolio API).");

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("Portofolio API: GET {PORTFOLIO_API_URL}...");
    let rows = fetch_portfolio_list(&http, &bearer).await?;
    println!("Portofolio: {} baris dari API.", rows.len());

    let mut codes: Vec<String> = rows
        .iter()
        .map(|r| r.emiten_name.trim().to_ascii_uppercase())
        .filter(|c| !c.is_empty())
        .collect();
    codes.sort();
    codes.dedup();

    if !codes.is_empty() {
        println!(
            "Portofolio: pastikan emiten_list untuk {} kode (skip bila update_at masih fresh)...",
            codes.len()
        );
        let list_ok =
            emiten_list_worker::scrape_and_insert_key_stats(page, session.as_ref(), keyspace, &codes)
                .await?;
        println!("Portofolio: emiten_list upsert/scrape OK={list_ok}.");

        let today = Local::now().date_naive();
        println!(
            "Portofolio: pastikan bandarmology untuk {} kode (skip bila agg hari ini ada)...",
            codes.len()
        );
        let bandar_ok = bandarmology_worker::scrape_and_insert_bandarmology(
            page, session, keyspace, today, &codes,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
        println!("Portofolio: bandarmology insert OK={bandar_ok}.");
    }

    let n = upsert_portofolio(session.as_ref(), keyspace, &rows).await?;
    println!("OK: {n} baris diinsert ke portofolio.");

    if !codes.is_empty() {
        println!(
            "Portofolio: salin bandarmology minggu berjalan → portofolio_bandarmology ({} kode)...",
            codes.len()
        );
        let pb_ok = portofolio_bandarmology_worker::insert_portofolio_bandarmology_for_emitens(
            session.as_ref(),
            keyspace,
            &codes,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
        println!("OK: {pb_ok}/{} baris diupsert ke portofolio_bandarmology.", codes.len());
    }

    println!("Portofolio: hapus orphan portofolio_bandarmology (tidak ada di portofolio)...");
    let del_ok = portofolio_bandarmology_worker::delete_unused_portofolio_bandarmology(
        session, keyspace,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    println!("OK: {del_ok} partition orphan dihapus dari portofolio_bandarmology.");

    Ok(n)
}

fn json_i32(v: &Value, path: &[&str]) -> i32 {
    json_i64(v, path) as i32
}

fn parse_order_time_key(item: &Value) -> Option<DateTime<Utc>> {
    for path in ["/time/match", "/time/open", "/time/order"] {
        let s = item.pointer(path).and_then(|x| x.as_str()).unwrap_or("").trim();
        if s.is_empty() || s.starts_with("0001-01-01") {
            continue;
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    None
}

/// Parse `data[]` dari order/v2/list → map history (key = waktu match/open/order).
pub fn parse_order_history_map(
    v: &Value,
) -> HashMap<DateTime<Utc>, PortofolioHistoryItemUd> {
    let items = v
        .get("data")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut out = HashMap::new();
    for item in items {
        let Some(ts) = parse_order_time_key(&item) else {
            continue;
        };
        let order_id = item
            .get("order_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if order_id.is_empty() {
            continue;
        }
        let symbol = item
            .get("symbol")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        let message = item
            .get("message")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let side = item
            .get("side")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        out.insert(
            ts,
            PortofolioHistoryItemUd {
                order_id,
                message,
                symbol,
                side,
                lot_done: json_i32(&item, &["qty", "lot_done"]),
                price_average: json_f64(&item, &["price", "average", "price"]),
                amount_matched: json_f64(&item, &["amount", "matched"]),
            },
        );
    }
    out
}

async fn fetch_order_history_by_stock_code(
    http: &reqwest::Client,
    bearer: &str,
    stock_code: &str,
) -> Result<HashMap<DateTime<Utc>, PortofolioHistoryItemUd>, Box<dyn std::error::Error>> {
    let url = format!(
        "{ORDER_LIST_API_URL}?filter_criteria.stock_code={}",
        urlencoding_simple(stock_code)
    );
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
        .send()
        .await?;

    let status = resp.status();
    crate::http_abort::abort_app_if_http_4xx(status, "order/v2/list?filter_criteria.stock_code");
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("order/v2/list (stock={stock_code}) HTTP {status}: {preview}").into());
    }
    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("order/v2/list JSON: {e}"))?;
    Ok(parse_order_history_map(&v))
}

fn urlencoding_simple(s: &str) -> String {
    // stock code = huruf saja; cukup escape aman.
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

/// Timpa `portofolio.history` untuk satu emiten (bukan push/merge).
pub async fn replace_portofolio_history(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    history: &HashMap<DateTime<Utc>, PortofolioHistoryItemUd>,
) -> Result<(), Box<dyn std::error::Error>> {
    let code = emiten_name.trim().to_ascii_uppercase();
    let update = session
        .prepare(format!(
            "UPDATE {keyspace}.portofolio SET history = ? WHERE emiten_name = ?"
        ))
        .await?;
    session
        .execute_unpaged(&update, (history, code.as_str()))
        .await?;
    Ok(())
}

/// START TRADING/PIN → Bearer trading → GET order/v2/list?filter_criteria.stock_code=
/// → timpa `portofolio.history`. Returns jumlah entri history.
pub async fn scrape_and_replace_portofolio_history(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet".into());
    }

    ensure_trading_session(page).await?;
    println!("Portofolio history: jeda 2 detik setelah PIN / mode trading siap...");
    sleep(Duration::from_secs(2)).await;

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!(
        "Portofolio history API: GET {ORDER_LIST_API_URL}?filter_criteria.stock_code={code}..."
    );
    let history = fetch_order_history_by_stock_code(&http, &bearer, &code).await?;
    let n = history.len();
    replace_portofolio_history(session, keyspace, &code, &history).await?;
    println!("OK: portofolio.history {code} ditimpa dengan {n} entri.");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_portfolio_maps_lots_and_gain_percent() {
        let v: Value = serde_json::from_str(
            r#"{
                "data": {
                    "results": [
                        {
                            "symbol": "AMRT",
                            "company": {
                                "name": "Sumber Alfaria Trijaya Tbk",
                                "icon_url": "https://assets.stockbit.com/logos/companies/AMRT.png"
                            },
                            "qty": {
                                "available": { "lot": 24 },
                                "balance": { "lot": 24 }
                            },
                            "price": {
                                "latest": 1335,
                                "average": { "price": 1361.414 }
                            },
                            "asset": {
                                "amount_invested": 3267393.6,
                                "unrealised": {
                                    "market_value": 3204000,
                                    "profit_loss": -63393.6,
                                    "gain": -0.019401
                                }
                            }
                        }
                    ]
                }
            }"#,
        )
        .unwrap();
        let rows = parse_portfolio_list_json(&v);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.emiten_name, "AMRT");
        assert_eq!(r.long_name, "Sumber Alfaria Trijaya Tbk");
        assert_eq!(r.balance_lot, 24);
        assert_eq!(r.available_lot, 24);
        assert!((r.average_price - 1361.414).abs() < 1e-6);
        assert!((r.current_price - 1335.0).abs() < 1e-9);
        assert!((r.percentage - (-1.9401)).abs() < 1e-6);
    }

    #[test]
    fn parse_order_history_maps_fields() {
        let v: Value = serde_json::from_str(
            r#"{
                "message": "Orders data retrieved",
                "data": [
                    {
                        "order_id": "XL46376141PYwb3yIzZ7",
                        "symbol": "ASBI",
                        "message": "",
                        "side": "SIDE_BUY",
                        "time": {
                            "order": "2026-07-20T11:17:01Z",
                            "open": "2026-07-20T11:17:01Z",
                            "match": "2026-07-20T07:20:55Z"
                        },
                        "qty": { "lot_done": 15, "lot_open": 0 },
                        "price": { "order": 424, "average": { "price": 424 } },
                        "amount": { "matched": 636000 }
                    }
                ]
            }"#,
        )
        .unwrap();
        let map = parse_order_history_map(&v);
        assert_eq!(map.len(), 1);
        let (ts, item) = map.iter().next().unwrap();
        assert_eq!(item.order_id, "XL46376141PYwb3yIzZ7");
        assert_eq!(item.symbol, "ASBI");
        assert_eq!(item.side, "SIDE_BUY");
        assert_eq!(item.lot_done, 15);
        assert!((item.price_average - 424.0).abs() < 1e-9);
        assert!((item.amount_matched - 636000.0).abs() < 1e-9);
        assert_eq!(ts.timestamp(), 1_784_532_055); // 2026-07-20T07:20:55Z
    }
}
