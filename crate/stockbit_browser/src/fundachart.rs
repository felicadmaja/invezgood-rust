//! Fundachart PBV band — `exodus.api.stockbit.com/fundachart/data` (sesi terminal/desktop).

use std::time::Duration;

use chromiumoxide::page::Page;
use serde::Deserialize;
use tokio::time::sleep;

use crate::{
    acquire_browser_session, evaluate_resilient, extract_stockbit_bearer, goto_stockbit,
    launch_page, open_stream_or_login, bearer_http_client, BrowserLockClass, StockbitError,
};

const TERMINAL_ORIGIN: &str = "https://terminal.stockbit.com";
const FUNDACHART_DATA_HOST: &str = "https://exodus.api.stockbit.com/fundachart/data";

fn install_fundachart_capture_js() -> String {
    r#"(() => {
        if (window.__sbFundachartCaptureInstalled) return 0;
        window.__sbFundachartCaptureInstalled = true;
        window.__sbFundachartJson = window.__sbFundachartJson || '';
        const remember = (text) => {
            if (text && text.includes('"chart_entry"')) window.__sbFundachartJson = text;
        };
        const ofetch = window.fetch;
        window.fetch = async function (input, init) {
            const url = typeof input === 'string' ? input : (input && input.url) || '';
            const res = await ofetch.apply(this, arguments);
            if (url.includes('fundachart/data')) {
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
                    if (u.includes('fundachart/data') && this.responseText)
                        remember(this.responseText);
                } catch (_) {}
            });
            return oSend.apply(this, arguments);
        };
        return 1;
    })()"#
    .to_string()
}

async fn cookie_header(page: &Page) -> String {
    let Ok(cookies) = page.get_cookies().await else {
        return String::new();
    };
    cookies
        .iter()
        .map(|c| format!("{}={}", c.name, c.value))
        .collect::<Vec<_>>()
        .join("; ")
}

fn desktop_headers(bearer: &str, cookie: &str) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, COOKIE, ORIGIN, REFERER};
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
    if let Ok(v) = HeaderValue::from_str(&format!("Bearer {bearer}")) {
        headers.insert(AUTHORIZATION, v);
    }
    if !cookie.is_empty() {
        if let Ok(v) = HeaderValue::from_str(cookie) {
            headers.insert(COOKIE, v);
        }
    }
    headers.insert(ORIGIN, HeaderValue::from_static("https://terminal.stockbit.com"));
    headers.insert(REFERER, HeaderValue::from_static("https://terminal.stockbit.com/"));
    headers.insert("x-platform", HeaderValue::from_static("desktop"));
    headers.insert("x-appversion", HeaderValue::from_static("2.8.1"));
    headers
}

#[derive(Debug, Deserialize)]
struct TokenEnvelope {
    #[serde(default)]
    data: serde_json::Value,
    #[serde(default)]
    token: Option<String>,
}

fn extract_token_from_json(body: &str) -> Option<String> {
    let parsed: TokenEnvelope = serde_json::from_str(body).ok()?;
    if let Some(t) = parsed.token.filter(|t| t.len() >= 32) {
        return Some(t);
    }
    let data = parsed.data;
    if let Some(s) = data.as_str().filter(|t| t.len() >= 32) {
        return Some(s.to_string());
    }
    data.get("token")
        .or_else(|| data.get("hash"))
        .and_then(|v| v.as_str())
        .filter(|t| t.len() >= 32)
        .map(|s| s.to_string())
}

async fn resolve_fundachart_token(
    client: &reqwest::Client,
    code: &str,
    headers: &reqwest::header::HeaderMap,
) -> Result<String, String> {
    let get_urls = [
        format!("https://exodus.api.stockbit.com/fundachart/token?symbol={code}"),
        format!("https://exodus.api.stockbit.com/fundachart/token/{code}"),
        format!("https://exodus.stockbit.com/fundachart/token?symbol={code}"),
        format!("https://exodus.stockbit.com/fundachart/data/token?symbol={code}"),
    ];
    for url in get_urls {
        let resp = client
            .get(&url)
            .headers(headers.clone())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            continue;
        }
        let body = resp.text().await.map_err(|e| e.to_string())?;
        if let Some(token) = extract_token_from_json(&body) {
            return Ok(token);
        }
    }

    let posts: [(&str, serde_json::Value); 3] = [
        (
            "https://exodus.api.stockbit.com/fundachart/token",
            serde_json::json!({ "symbol": code }),
        ),
        (
            "https://exodus.api.stockbit.com/fundachart/data/token",
            serde_json::json!({ "symbol": code, "metric": "pbv" }),
        ),
        (
            "https://exodus.stockbit.com/fundachart/run",
            serde_json::json!({ "symbol": code, "metric_name": "pbv" }),
        ),
    ];
    for (url, body) in posts {
        let resp = client
            .post(url)
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            continue;
        }
        let text = resp.text().await.map_err(|e| e.to_string())?;
        if let Some(token) = extract_token_from_json(&text) {
            return Ok(token);
        }
    }

    Err(format!(
        "token fundachart {code} tidak ditemukan dari API Stockbit (cek paywall / terminal PBV)"
    ))
}

async fn fetch_fundachart_data(
    client: &reqwest::Client,
    token: &str,
    headers: &reqwest::header::HeaderMap,
) -> Result<String, String> {
    let url = format!("{FUNDACHART_DATA_HOST}?token={token}");
    let resp = client
        .get(&url)
        .headers(headers.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("fundachart/data HTTP {}: {}", status, &body[..body.len().min(300)]));
    }
    if !body.contains("chart_entry") {
        return Err(format!(
            "fundachart/data tanpa chart_entry: {}",
            &body[..body.len().min(300)]
        ));
    }
    Ok(body)
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

/// Cek Redis `stockbit:readiness` — wajib `ready=true` (login Chrome OK).
pub async fn require_stockbit_ready() -> Result<(), StockbitError> {
    match crate::redis_readiness::get().await {
        Some(u) if u.ready => Ok(()),
        Some(u) => Err(format!("Stockbit belum ready: {}", u.message).into()),
        None => Err("Stockbit readiness tidak diketahui (Redis kosong atau down)".into()),
    }
}

/// JSON mentah respons `fundachart/data` (PBV band) untuk emiten `code`.
pub async fn fetch_fundachart_pbv_json(code: &str) -> Result<String, StockbitError> {
    require_stockbit_ready().await?;

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
    let _ = page.evaluate(crate::ensure_auth_capture_js()).await;
    let bearer = extract_stockbit_bearer(&page).await?;
    let cookie = cookie_header(&page).await;

    let symbol_path = format!("https://stockbit.com/symbol/{code}/keystats");
    goto_stockbit(&page, &symbol_path).await?;
    sleep(Duration::from_secs(2)).await;
    let _ = goto_stockbit(&page, TERMINAL_ORIGIN).await;
    sleep(Duration::from_secs(3)).await;

    if let Some(body) = poll_captured_json(&page, 20).await {
        browser.close().await;
        return Ok(body);
    }

    let client = bearer_http_client().await?;
    let headers = desktop_headers(&bearer, &cookie);

    let result = async {
        let token = resolve_fundachart_token(&client, &code, &headers).await?;
        fetch_fundachart_data(&client, &token, &headers).await
    }
    .await;

    browser.close().await;
    result.map_err(|e| -> StockbitError { e.into() })
}
