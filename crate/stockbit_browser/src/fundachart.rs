//! Fundachart PBV band — web scrape DOM `terminal.stockbit.com` (intercept XHR fundachart).

use std::time::{Duration, Instant};

use chromiumoxide::page::Page;
use tokio::time::sleep;

use crate::{
    acquire_browser_session, evaluate_resilient, goto_stockbit, launch_page, open_stream_or_login,
    BrowserLockClass, StockbitError,
};

const TERMINAL_MAIN_URL: &str = "https://terminal.stockbit.com/main?";

fn install_fundachart_capture_js() -> String {
    r#"(() => {
        if (window.__sbFundachartCaptureInstalled) return 0;
        window.__sbFundachartCaptureInstalled = true;
        window.__sbFundachartJson = window.__sbFundachartJson || '';
        const remember = (text) => {
            if (!text || typeof text !== 'string') return;
            if (
                text.includes('"chart_entry"')
                || text.includes('Fundamental chart data retrieved')
            ) {
                window.__sbFundachartJson = text;
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

async fn poll_captured_json(page: &Page, attempts: u32) -> Option<String> {
    for _ in 0..attempts {
        let raw = evaluate_resilient(page, r#"(() => window.__sbFundachartJson || '')()"#)
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

async fn scrape_pbv_via_terminal_dom(page: &Page, code: &str) -> Result<String, StockbitError> {
    goto_stockbit(page, TERMINAL_MAIN_URL).await?;
    wait_document_ready(page, 60).await?;

    type_symbol_and_submit(page, code).await?;
    println!("fundachart {code}: Search Symbol → Enter");
    wait_document_ready(page, 45).await?;

    click_by_visible_text(page, &["div", "button", "span"], "Analysis").await?;
    println!("fundachart {code}: klik Analysis");
    wait_document_ready(page, 45).await?;

    click_by_visible_text(page, &["button"], "5 Years").await?;
    println!("fundachart {code}: klik 5 Years — pantau XHR fundachart");

    poll_captured_json(page, 80).await.ok_or_else(|| {
        StockbitError::from(format!(
            "fundachart {code}: tidak ada respons XHR chart_entry setelah 5 Years"
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

/// JSON mentah respons fundachart (XHR) untuk emiten `code` — scrape DOM terminal.
pub async fn fetch_fundachart_pbv_json(code: &str) -> Result<String, StockbitError> {
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

    let _ = evaluate_resilient(&page, install_fundachart_capture_js()).await;
    let body = scrape_pbv_via_terminal_dom(&page, &code).await?;
    browser.close().await;
    Ok(body)
}
