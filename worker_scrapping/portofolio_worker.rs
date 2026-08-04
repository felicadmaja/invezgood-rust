//! Portfolio via API `carina.stockbit.com/portfolio/v2/list`:
//! - `data.results` → upsert `portofolio`
//! - `data.summary` → upsert `portofolio_equity` (satu GET untuk keduanya)
//!
//! Alur: bila tombol START TRADING masih ada → klik → input PIN (`STOCKBUT_PIN` /
//! `STOCKBIT_PIN`) → Submit → tunggu modal hilang.
//! Ambil **Bearer trading** (pasca-PIN / `securitiesAccessToken`),
//! **bukan** Bearer login web Exodus, lalu GET portfolio API.
//!
//! Icon/long_name dari API portfolio langsung (tanpa emiten_list/GCS).

use chromiumoxide::page::Page;
use rand::Rng;
use scylla::client::session::Session;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stockbit_browser::goto_stockbit;
use tokio::time::sleep;

use crate::portofolio_equity_worker;

const PORTFOLIO_API_URL: &str = "https://carina.stockbit.com/portfolio/v2/list";
const STOCKBIT_PORTFOLIO_URL: &str = "https://stockbit.com/securities/portfolio";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

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
    /// Persentase (gain API × 100), contoh gain `-0.019401` → `-1.9401`.
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

fn resolve_portofolio_long_name(
    _session: &Session,
    _keyspace: &str,
    _emiten: &str,
    api_long_name: &str,
) -> String {
    api_long_name.trim().to_string()
}

fn resolve_portofolio_icon(
    _session: &Session,
    _keyspace: &str,
    _emiten: &str,
    icon_url: &str,
) -> String {
    icon_url.trim().to_string()
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
/// Returns `true` jika PIN baru saja diinput; `false` jika sudah mode trading.
pub async fn ensure_trading_session(page: &Page) -> Result<bool, Box<dyn std::error::Error>> {
    if !is_start_trading_visible(page).await? {
        println!("START TRADING tidak terlihat — sudah mode trading, lewati PIN.");
        return Ok(false);
    }

    println!("Portofolio: klik START TRADING...");
    click_start_trading(page).await?;
    let pin = trading_pin()?;
    wait_for_pin_modal(page, Duration::from_secs(30)).await?;
    type_pin_natural(page, &pin).await?;
    click_pin_submit(page).await?;
    wait_pin_modal_gone(page, Duration::from_secs(45)).await?;
    Ok(true)
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

    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(45))
        .build()?;

    for h in &candidates {
        let token = h.get("token").and_then(|x| x.as_str()).unwrap_or("");
        if !token.starts_with("eyJ") {
            continue;
        }
        match probe_trading_bearer(&http, token).await {
            Ok(status) if (200..300).contains(&status) => {
                return Ok(token.to_string());
            }
            _ => {}
        }
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

async fn fetch_portfolio_json(
    http: &reqwest::Client,
    bearer: &str,
) -> Result<(Value, crate::rate_limit_delay::RateLimitInfo), Box<dyn std::error::Error>> {
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
    let rate_info = crate::rate_limit_delay::RateLimitInfo::from_headers(resp.headers());
    let rate = crate::rate_limit_delay::rate_limit_headers_log(resp.headers());
    println!("  portfolio/v2/list → HTTP {status} | {rate}");
    crate::http_abort::abort_app_if_http_4xx(status, "portfolio/v2/list");
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("portfolio/v2/list HTTP {status}: {preview} | {rate}").into());
    }

    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("portfolio/v2/list JSON: {e}"))?;
    Ok((v, rate_info))
}

async fn fetch_portfolio_list(
    http: &reqwest::Client,
    bearer: &str,
) -> Result<
    (
        Value,
        Vec<PortoRow>,
        crate::rate_limit_delay::RateLimitInfo,
    ),
    Box<dyn std::error::Error>,
> {
    let (v, rate_info) = fetch_portfolio_json(http, bearer).await?;
    let rows = parse_portfolio_list_json(&v);
    if rows.is_empty() {
        return Err("portfolio/v2/list: data.results kosong".into());
    }
    Ok((v, rows, rate_info))
}

async fn fetch_portfolio_list_string(
    http: &reqwest::Client,
    bearer: &str,
) -> Result<
    (
        Value,
        Vec<PortoRow>,
        crate::rate_limit_delay::RateLimitInfo,
    ),
    String,
> {
    fetch_portfolio_list(http, bearer)
        .await
        .map_err(|e| e.to_string())
}

async fn truncate_portofolio(
    session: &Session,
    keyspace: &str,
) -> Result<(), String> {
    println!("Portofolio: TRUNCATE {keyspace}.portofolio...");
    session
        .query_unpaged(format!("TRUNCATE {keyspace}.portofolio"), &[])
        .await
        .map_err(|e| format!("TRUNCATE {keyspace}.portofolio: {e}"))?;
    println!("Portofolio: truncate selesai.");
    Ok(())
}

async fn insert_portofolio(
    session: &Session,
    keyspace: &str,
    rows: &[PortoRow],
) -> Result<usize, Box<dyn std::error::Error>> {
    println!("Portofolio: mulai insert {} baris...", rows.len());

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
            resolve_portofolio_icon(session, keyspace, &emiten, &row.emiten_icon_url);
        let long_name =
            resolve_portofolio_long_name(session, keyspace, &emiten, &row.long_name);

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
        if n > 1 {
            println!();
        }
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

/// START TRADING (opsional) → PIN (opsional) → Bearer trading →
/// GET `portfolio/v2/list` → upsert `portofolio_equity` dari `data.summary`
/// **dan** upsert `portofolio` dari `data.results`.
/// Bila `with_bandarmology`: pastikan `bandarmology` untuk kode holdings.
/// Tidak menulis `portofolio_bandarmology`.
/// Returns `(jumlah baris portofolio, daftar emiten_name)`.
pub async fn scrape_and_insert_portofolio(
    page: &Page,
    session: &Arc<Session>,
    keyspace: &str,
    with_bandarmology: bool,
) -> Result<(usize, Vec<String>), Box<dyn std::error::Error>> {
    let pin_entered = ensure_trading_session(page).await?;
    if pin_entered {
        println!("Jeda 1 detik setelah input PIN...");
        sleep(Duration::from_secs(1)).await;
    }

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("Portofolio: GET {PORTFOLIO_API_URL}...");
    let (json, rows, rate) = fetch_portfolio_list_string(&http, &bearer).await?;
    let delay_ms = rate.inter_emiten_delay_ms();
    if delay_ms > 0 {
        println!(
            "  jeda adaptif {delay_ms}ms (portfolio/v2/list; remaining={}; {})",
            rate.remaining
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            rate.log_line()
        );
        sleep(Duration::from_millis(delay_ms)).await;
    }

    println!("Portofolio: {} baris dari data.results — truncate lalu insert...", rows.len());
    truncate_portofolio(session.as_ref(), keyspace).await?;

    println!("Portofolio equity: upsert dari data.summary API...");
    let equity_ok = portofolio_equity_worker::upsert_portofolio_equity_from_json(
        session.as_ref(),
        keyspace,
        &json,
    )
    .await?;
    println!("OK: {equity_ok} baris diupsert ke portofolio_equity.");

    let mut codes: Vec<String> = rows
        .iter()
        .map(|r| r.emiten_name.trim().to_ascii_uppercase())
        .filter(|c| !c.is_empty())
        .collect();
    codes.sort();
    codes.dedup();

    let _ = with_bandarmology;

    let n = insert_portofolio(session.as_ref(), keyspace, &rows).await?;
    println!("OK: {n} baris diinsert ke portofolio.");

    Ok((n, codes))
}

/// START TRADING (opsional) → PIN (opsional) → Bearer trading →
/// GET `portfolio/v2/list` → upsert hanya `portofolio_equity` dari `data.summary`.
pub async fn scrape_and_insert_portofolio_equity(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let pin_entered = ensure_trading_session(page).await?;
    if pin_entered {
        println!("Jeda 1 detik setelah input PIN...");
        sleep(Duration::from_secs(1)).await;
    }

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("Portofolio equity API: GET {PORTFOLIO_API_URL} (data.summary)...");
    let (json, rate) = fetch_portfolio_json(&http, &bearer).await?;
    let delay_ms = rate.inter_emiten_delay_ms();
    if delay_ms > 0 {
        println!(
            "  jeda adaptif {delay_ms}ms (portfolio/v2/list equity; remaining={}; {})",
            rate.remaining
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            rate.log_line()
        );
        sleep(Duration::from_millis(delay_ms)).await;
    }

    portofolio_equity_worker::upsert_portofolio_equity_from_json(session, keyspace, &json).await
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
}
