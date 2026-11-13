//! Fundachart PBV band — web scrape DOM `terminal.stockbit.com` (intercept XHR fundachart).

use std::time::{Duration, Instant};

use chromiumoxide::page::Page;
use tokio::time::sleep;

use crate::{
    acquire_browser_session, evaluate_resilient, goto_stockbit, launch_page, open_stream_or_login,
    BrowserLockClass, StockbitError,
};

#[allow(dead_code)]
const TERMINAL_MAIN_URL: &str = "https://terminal.stockbit.com/main?";

fn install_fundachart_capture_js() -> String {
    r#"(() => {
        if (window.__sbFundachartCaptureInstalled) return 0;
        window.__sbFundachartCaptureInstalled = true;
        window.__sbFundachartJson = window.__sbFundachartJson || '';
        window.__sbFundachartPE = window.__sbFundachartPE || '';
        window.__sbFundachartPBV = window.__sbFundachartPBV || '';
        const remember = (text) => {
            if (!text || typeof text !== 'string') return;
            if (
                !text.includes('"chart_entry"')
                && !text.includes('Fundamental chart data retrieved')
            ) {
                return;
            }
            window.__sbFundachartJson = text;
            // Routing PE vs PBV berdasar judul seri di payload.
            if (text.includes('PE Ratio') || text.includes('PE Standard Deviation')) {
                window.__sbFundachartPE = text;
            } else if (
                text.includes('Price to Book') || text.includes('PBV Standard Deviation')
            ) {
                window.__sbFundachartPBV = text;
            }
        };
        const urlHit = (u) =>
            u.includes('fundachart') || u.includes('fundamental') || u.includes('Fundachart');
        const ofetch = window.fetch;
        window.fetch = async function (input, init) {
            const url = typeof input === 'string' ? input : (input && input.url) || '';
            const res = await ofetch.apply(this, arguments);
            if (urlHit(url)) {
                try { remember(await res.clone().text()); } catch (_) {}
            }
            return res;
        };
        const oOpen = XMLHttpRequest.prototype.open;
        XMLHttpRequest.prototype.open = function (method, url) {
            this.__sbFundachartUrl = url;
            return oOpen.apply(this, arguments);
        };
        const oSend = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.send = function () {
            this.addEventListener('load', function () {
                try {
                    const u = String(this.__sbFundachartUrl || '');
                    if (urlHit(u) && this.responseText) remember(this.responseText);
                } catch (_) {}
            });
            return oSend.apply(this, arguments);
        };
        return 1;
    })()"#
    .to_string()
}

async fn wait_document_ready(page: &Page, timeout_secs: u64) -> Result<(), StockbitError> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(timeout_secs) {
        let ready = evaluate_resilient(page, r#"(() => document.readyState === 'complete')()"#)
            .await
            .ok()
            .and_then(|v| v.into_value::<bool>().ok())
            .unwrap_or(false);
        if ready {
            sleep(Duration::from_millis(900)).await;
            return Ok(());
        }
        sleep(Duration::from_millis(300)).await;
    }
    Err("timeout menunggu halaman terminal load".into())
}

#[allow(dead_code)]
async fn click_by_visible_text(page: &Page, tags: &[&str], text: &str) -> Result<(), StockbitError> {
    let tags_js = tags
        .iter()
        .map(|t| format!("{t:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let text_js = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into());
    let script = format!(
        r#"(() => {{
            const want = {text_js};
            const tags = [{tags_js}];
            for (const tag of tags) {{
                for (const el of document.querySelectorAll(tag)) {{
                    const t = (el.textContent || '').trim();
                    if (t === want) {{
                        el.click();
                        return true;
                    }}
                }}
            }}
            for (const el of document.querySelectorAll('div, button, span, a')) {{
                const t = (el.textContent || '').trim();
                if (t === want) {{
                    el.click();
                    return true;
                }}
            }}
            return false;
        }})()"#
    );
    let ok = evaluate_resilient(page, script)
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(format!("elemen teks '{text}' tidak ditemukan / tidak bisa diklik").into())
    }
}

/// URL terminal langsung ke symbol (tanpa perlu ketik Search Symbol).
fn terminal_symbol_url(code: &str) -> String {
    format!("https://terminal.stockbit.com/main?layout=watchlist&tabName={code}&symbol={code}")
}

/// Klik teks dengan retry (SPA butuh waktu mount tab Analysis / tombol chart).
#[allow(dead_code)]
async fn click_by_visible_text_retry(
    page: &Page,
    tags: &[&str],
    text: &str,
    attempts: u32,
) -> Result<(), StockbitError> {
    let mut last = StockbitError::from(format!("elemen teks '{text}' tidak ditemukan"));
    for _ in 0..attempts {
        match click_by_visible_text(page, tags, text).await {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
        sleep(Duration::from_millis(800)).await;
    }
    Err(last)
}

/// Klik tab Analysis (target class spesifik `.css-1ll1mc4`/`e2ln8a0`, full pointer events),
/// retry sampai tombol "5 Years" muncul (panel chart terbuka).
async fn click_analysis_tab(page: &Page, attempts: u32) -> Result<(), StockbitError> {
    let click_js = r#"(() => {
        const vis = (el) => {
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
        };
        const fire = (el) => {
            for (const t of ['pointerdown','mousedown','pointerup','mouseup','click']) {
                el.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true, view: window }));
            }
        };
        const sels = ['.css-1ll1mc4','[class*="e2ln8a0"]','[role="tab"]','button','a','span','div'];
        for (const sel of sels) {
            for (const el of document.querySelectorAll(sel)) {
                if (vis(el) && (el.textContent || '').trim() === 'Analysis') { fire(el); return true; }
            }
        }
        return false;
    })()"#;
    let has_5y_js = r#"(() => {
        for (const el of document.querySelectorAll('button, [role="button"]')) {
            if ((el.textContent || '').trim() === '5 Years') return true;
        }
        return false;
    })()"#;
    for _ in 0..attempts {
        let _ = evaluate_resilient(page, click_js).await;
        sleep(Duration::from_millis(900)).await;
        let has = evaluate_resilient(page, has_5y_js)
            .await
            .ok()
            .and_then(|v| v.into_value::<bool>().ok())
            .unwrap_or(false);
        if has {
            return Ok(());
        }
    }
    Err("tab Analysis: tombol '5 Years' tidak muncul setelah klik".into())
}

/// Klik semua tombol dengan retry sampai minimal satu ter-klik (return total terakhir).
async fn click_all_buttons_by_text_retry(page: &Page, text: &str, attempts: u32) -> u32 {
    let mut total = 0;
    for _ in 0..attempts {
        total = click_all_buttons_by_text(page, text).await.unwrap_or(0);
        if total > 0 {
            break;
        }
        sleep(Duration::from_millis(800)).await;
    }
    total
}

#[allow(dead_code)]
async fn type_symbol_and_submit(page: &Page, code: &str) -> Result<(), StockbitError> {
    let selector = r#"input[placeholder="Search Symbol"]"#;
    let element = page
        .find_element(selector)
        .await
        .map_err(|_| -> StockbitError {
            "input Search Symbol tidak ditemukan di terminal.stockbit.com".into()
        })?;

    element.click().await?;
    sleep(Duration::from_millis(400)).await;

    let clear_js = format!(
        r#"(() => {{
            const el = document.querySelector({selector:?});
            if (!el) return false;
            el.focus();
            el.value = '';
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
            return true;
        }})()"#
    );
    let _ = page.evaluate(clear_js).await;

    for ch in code.chars() {
        element.type_str(&ch.to_string()).await?;
        sleep(Duration::from_millis(90)).await;
    }
    sleep(Duration::from_millis(300)).await;
    element.press_key("Enter").await.map_err(|e| -> StockbitError {
        format!("Enter pada Search Symbol gagal: {e}").into()
    })?;
    Ok(())
}

/// Kosongkan buffer capture (PE/PBV/last) tanpa melepas interceptor — dipakai sebelum klik
/// "5 Years" supaya poll hanya menangkap respons timeframe 5 tahun (bukan default 3 tahun).
fn reset_captured_json_js() -> &'static str {
    r#"(() => {
        window.__sbFundachartPE = '';
        window.__sbFundachartPBV = '';
        window.__sbFundachartJson = '';
        return 1;
    })()"#
}

/// Jenis fundachart band yang di-scrape dari tab Analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundachartKind {
    PeBand,
    PbvBand,
}

impl FundachartKind {
    fn window_var(self) -> &'static str {
        match self {
            FundachartKind::PeBand => "window.__sbFundachartPE",
            FundachartKind::PbvBand => "window.__sbFundachartPBV",
        }
    }

    fn label(self) -> &'static str {
        match self {
            FundachartKind::PeBand => "PE band",
            FundachartKind::PbvBand => "PBV band",
        }
    }
}

/// Klik SEMUA elemen `button` dengan teks persis `text` (tab Analysis punya 2 tombol "5 Years":
/// PE Band & PBV Band). Return jumlah yang diklik.
async fn click_all_buttons_by_text(page: &Page, text: &str) -> Result<u32, StockbitError> {
    let text_js = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into());
    let script = format!(
        r#"(() => {{
            const want = {text_js};
            const vis = (el) => {{
                const r = el.getBoundingClientRect();
                const s = getComputedStyle(el);
                return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
            }};
            const fire = (el) => {{
                for (const t of ['pointerdown','mousedown','pointerup','mouseup','click']) {{
                    el.dispatchEvent(new MouseEvent(t, {{ bubbles: true, cancelable: true, view: window }}));
                }}
            }};
            let n = 0;
            for (const el of document.querySelectorAll('button, [role="button"]')) {{
                if (vis(el) && (el.textContent || '').trim() === want) {{ fire(el); n++; }}
            }}
            return n;
        }})()"#
    );
    let n = evaluate_resilient(page, script)
        .await?
        .into_value::<i64>()
        .unwrap_or(0);
    Ok(n as u32)
}

async fn poll_captured_json(page: &Page, kind: FundachartKind, attempts: u32) -> Option<String> {
    let js = format!("(() => {} || '')()", kind.window_var());
    for _ in 0..attempts {
        let raw = evaluate_resilient(page, &js)
            .await
            .ok()
            .and_then(|v| v.into_value::<String>().ok())
            .unwrap_or_default();
        if raw.contains("chart_entry") {
            return Some(raw);
        }
        sleep(Duration::from_millis(500)).await;
    }
    None
}

async fn scrape_fundachart_via_terminal_dom(
    page: &Page,
    code: &str,
    kind: FundachartKind,
) -> Result<String, StockbitError> {
    goto_stockbit(page, &terminal_symbol_url(code)).await?;
    wait_document_ready(page, 60).await?;
    sleep(Duration::from_secs(3)).await; // beri waktu SPA mount tab/chart
    // Install interceptor SETELAH dokumen terminal load (navigasi me-reset window & fetch/XHR).
    let _ = evaluate_resilient(page, install_fundachart_capture_js()).await;
    println!("fundachart {code}: terminal symbol URL load + interceptor terpasang");

    click_analysis_tab(page, 20).await?;
    println!("fundachart {code}: tab Analysis terbuka (5 Years tersedia)");
    sleep(Duration::from_secs(1)).await;

    // Reset capture: buang respons timeframe default (3 thn) agar poll hanya menangkap respons 5 thn.
    let _ = evaluate_resilient(page, reset_captured_json_js()).await;
    let n = click_all_buttons_by_text_retry(page, "5 Years", 15).await;
    println!(
        "fundachart {code}: klik 5 Years (x{n}) — pantau XHR fundachart 5 tahun ({})",
        kind.label()
    );

    poll_captured_json(page, kind, 80).await.ok_or_else(|| {
        StockbitError::from(format!(
            "fundachart {code}: tidak ada respons XHR {} setelah 5 Years",
            kind.label()
        ))
    })
}

/// Cek Redis `stockbit:readiness` — wajib `ready=true` (login Chrome OK).
pub async fn require_stockbit_ready() -> Result<(), StockbitError> {
    match crate::redis_readiness::get().await {
        Some(u) if u.ready => Ok(()),
        Some(u) => Err(format!("Stockbit belum ready: {}", u.message).into()),
        None => Err("Stockbit readiness tidak diketahui (Redis kosong atau down)".into()),
    }
}

/// Gate readiness untuk RPC scrape interaktif: kontensi lock poller tidak memblokir
/// (`fetch` akan `acquire_browser_session(Interactive)` sendiri).
pub async fn require_stockbit_ready_for_scrape() -> Result<(), StockbitError> {
    match crate::redis_readiness::get().await {
        Some(u) if u.ready => Ok(()),
        Some(u) if crate::is_transient_chrome_lock_error(&u.message) => Ok(()),
        Some(u) => Err(format!("Stockbit belum ready: {}", u.message).into()),
        None => Err("Stockbit readiness tidak diketahui (Redis kosong atau down)".into()),
    }
}

/// JSON mentah respons fundachart (XHR) untuk emiten `code` + jenis band (PE / PBV) — scrape DOM
/// terminal.stockbit.com (login Chrome, klik Analysis → 5 Years, intercept XHR fundachart/data).
pub async fn fetch_fundachart_json(
    code: &str,
    kind: FundachartKind,
) -> Result<String, StockbitError> {
    require_stockbit_ready_for_scrape().await?;

    let code = code.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!("code tidak valid ({code})").into());
    }

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();
    if email.trim().is_empty() || password.is_empty() {
        return Err("STOCKBIT_EMAIL / STOCKBIT_PASSWORD wajib".into());
    }

    let _browser_guard = acquire_browser_session(BrowserLockClass::Interactive)
        .await
        .map_err(|e| -> StockbitError { e.into() })?;
    let (browser, page) = launch_page().await?;
    open_stream_or_login(&page, email.trim(), password.trim()).await?;

    let body = scrape_fundachart_via_terminal_dom(&page, &code, kind).await?;
    browser.close().await;
    Ok(body)
}

/// JSON mentah respons fundachart PBV band (XHR) untuk emiten `code` — scrape DOM terminal.
pub async fn fetch_fundachart_pbv_json(code: &str) -> Result<String, StockbitError> {
    fetch_fundachart_json(code, FundachartKind::PbvBand).await
}

/// Scrape SEKALI kunjungan terminal.stockbit.com → tangkap PE band & PBV band sekaligus
/// (tab Analysis punya kedua chart; klik semua tombol "5 Years"). Return (json_pe, json_pbv).
pub async fn fetch_fundachart_both_json(code: &str) -> Result<(String, String), StockbitError> {
    require_stockbit_ready_for_scrape().await?;

    let code = code.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!("code tidak valid ({code})").into());
    }

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();
    if email.trim().is_empty() || password.is_empty() {
        return Err("STOCKBIT_EMAIL / STOCKBIT_PASSWORD wajib".into());
    }

    let _browser_guard = acquire_browser_session(BrowserLockClass::Interactive)
        .await
        .map_err(|e| -> StockbitError { e.into() })?;
    let (browser, page) = launch_page().await?;
    open_stream_or_login(&page, email.trim(), password.trim()).await?;

    goto_stockbit(&page, &terminal_symbol_url(&code)).await?;
    wait_document_ready(&page, 60).await?;
    sleep(Duration::from_secs(3)).await; // beri waktu SPA mount tab/chart
    // Install interceptor SETELAH dokumen terminal load (navigasi me-reset window & fetch/XHR).
    let _ = evaluate_resilient(&page, install_fundachart_capture_js()).await;
    println!("fundachart {code}: terminal symbol URL load + interceptor terpasang");

    click_analysis_tab(&page, 20).await?;
    println!("fundachart {code}: tab Analysis terbuka (5 Years tersedia)");
    sleep(Duration::from_secs(1)).await;

    // Reset capture: buang respons timeframe default (3 thn) agar poll hanya menangkap respons 5 thn.
    let _ = evaluate_resilient(&page, reset_captured_json_js()).await;
    let n = click_all_buttons_by_text_retry(&page, "5 Years", 15).await;
    println!("fundachart {code}: klik 5 Years (x{n}) — pantau XHR PE + PBV (5 tahun)");

    // Poll kedua seri; PE & PBV di-route oleh install_fundachart_capture_js().
    let pe = poll_captured_json(&page, FundachartKind::PeBand, 80).await;
    let pbv = poll_captured_json(&page, FundachartKind::PbvBand, 20).await;
    browser.close().await;

    match (pe, pbv) {
        (Some(pe), Some(pbv)) => Ok((pe, pbv)),
        (pe, pbv) => Err(format!(
            "fundachart {code}: XHR tidak lengkap (PE={}, PBV={})",
            pe.is_some(),
            pbv.is_some()
        )
        .into()),
    }
}
