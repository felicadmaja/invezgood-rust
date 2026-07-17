//! Scrape Portfolio Stockbit → upsert `portofolio`.
//!
//! Alur: bila tombol START TRADING masih ada → klik → input PIN (`STOCKBUT_PIN` / `STOCKBIT_PIN`) → Submit.
//! Bila START TRADING sudah tidak ada → sudah mode trading, lewati PIN.
//! Setelah PIN hilang (atau sudah trading): jeda 2 detik → buka
//! https://stockbit.com/securities/portfolio → scrape tabel → INSERT Scylla
//! (icon di-upload ke GCS modul `stoksaham`).

use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use gcs::{download_and_upload_emiten_icon, GcsOAuthTokenCache, GcsSignedUrlRuntime};
use rand::Rng;
use scylla::client::session::Session;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use stockbit_browser::goto_stockbit;
use tokio::time::sleep;

const STOCKBIT_PORTFOLIO_URL: &str = "https://stockbit.com/securities/portfolio";

#[derive(Debug, Clone, Deserialize)]
struct PortoRow {
    emiten_name: String,
    emiten_icon_url: String,
    balance_lot: String,
    available_lot: String,
    average_price: String,
    current_price: String,
    invested: String,
    market_value: String,
    potential_p_l: String,
    percentage: String,
}

fn trading_pin() -> Result<String, Box<dyn std::error::Error>> {
    // Prefer STOCKBUT_PIN (sesuai .env), fallback STOCKBIT_PIN.
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

fn parse_i64_number(raw: &str) -> i64 {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    cleaned.parse().unwrap_or(0)
}

/// Hapus koma pemisah ribuan; pertahankan tanda `-` dan titik desimal.
fn parse_f64_number(raw: &str) -> f64 {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-' || *c == '.')
        .collect();
    cleaned.parse().unwrap_or(0.0)
}

fn parse_percentage(raw: &str) -> f64 {
    parse_f64_number(&raw.replace('%', ""))
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

/// Masuk mode trading: klik START TRADING + PIN bila perlu; lewati bila tombol sudah hilang.
async fn ensure_trading_session(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
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
    // Fokus + clear input PIN.
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

    // Prefer type via element API bila selector stabil.
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
                        // Coba enable bila value sudah terisi.
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

async fn save_portfolio_page_screenshot(
    page: &Page,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("screenshots");
    tokio::fs::create_dir_all(&dir).await?;
    let path = dir.join("stockbit_10_portofolio_page.png");
    page.save_screenshot(
        ScreenshotParams::builder()
            .format(CaptureScreenshotFormat::Png)
            .build(),
        &path,
    )
    .await?;
    println!("Screenshot halaman Portfolio: {}", path.display());
    Ok(path)
}

async fn wait_for_portfolio_ready(
    page: &Page,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const create = Array.from(document.querySelectorAll('div, span'))
                        .some((el) => {
                            const t = (el.innerText || el.textContent || '').trim();
                            return t === 'Create New Portfolio' || t.includes('Create New Portfolio');
                        });
                    const rows = document.querySelectorAll('tr[data-cy="porto-list-row-table"]').length;
                    return create || rows > 0;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            println!("Halaman Portfolio siap (Create New Portfolio / baris tabel).");
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Timeout menunggu halaman Portfolio".into());
        }
        sleep(Duration::from_millis(400)).await;
    }
}

async fn scrape_portfolio_table(page: &Page) -> Result<Vec<PortoRow>, Box<dyn std::error::Error>> {
    let json = page
        .evaluate(
            r#"(() => {
                const rows = Array.from(document.querySelectorAll('tr[data-cy="porto-list-row-table"]'));
                const out = rows.map((tr) => {
                    const tds = Array.from(tr.querySelectorAll('td'));
                    const cellText = (td) => (td ? (td.innerText || td.textContent || '').trim() : '');
                    const nameEl = tr.querySelector('p.sc-94b72927-1, p[family="bold"]');
                    let emiten_name = nameEl
                        ? (nameEl.innerText || '').trim().split(/\s+/)[0]
                        : '';
                    if (!emiten_name && tds[0]) {
                        emiten_name = cellText(tds[0]).split(/\s+/)[0] || '';
                    }
                    const img = tr.querySelector('img[src*="assets.stockbit.com/logos"], img[alt]');
                    const emiten_icon_url = img
                        ? (img.currentSrc || img.getAttribute('src') || '').trim()
                        : '';
                    return {
                        emiten_name,
                        emiten_icon_url,
                        balance_lot: cellText(tds[1]),
                        available_lot: cellText(tds[2]),
                        average_price: cellText(tds[3]),
                        current_price: cellText(tds[4]),
                        invested: cellText(tds[5]),
                        market_value: cellText(tds[6]),
                        potential_p_l: cellText(tds[7]),
                        percentage: cellText(tds[8]),
                    };
                }).filter((r) => r.emiten_name);
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let rows: Vec<PortoRow> = serde_json::from_str(&json)?;
    Ok(rows)
}

async fn upsert_portofolio(
    session: &Session,
    keyspace: &str,
    rows: &[PortoRow],
) -> Result<usize, Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio (\
                emiten_name, emiten_icon, balance_lot, available_lot, \
                average_price, current_price, invested, market_value, \
                potential_p_l, percentage\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let mut n = 0usize;
    for row in rows {
        let emiten = row.emiten_name.trim().to_ascii_uppercase();
        if emiten.is_empty() {
            continue;
        }
        let emiten_icon = match upload_emiten_icon_to_gcs(&emiten, &row.emiten_icon_url).await {
            Ok(path) => path,
            Err(e) => {
                eprintln!("Peringatan: gagal upload icon GCS {emiten}: {e}");
                String::new()
            }
        };
        let balance_lot = parse_i64_number(&row.balance_lot);
        let available_lot = parse_i64_number(&row.available_lot);
        let average_price = parse_f64_number(&row.average_price);
        let current_price = parse_f64_number(&row.current_price);
        let invested = parse_f64_number(&row.invested);
        let market_value = parse_f64_number(&row.market_value);
        let potential_p_l = parse_f64_number(&row.potential_p_l);
        let percentage = parse_percentage(&row.percentage);

        session
            .execute_unpaged(
                &insert,
                (
                    emiten.as_str(),
                    emiten_icon.as_str(),
                    balance_lot,
                    available_lot,
                    average_price,
                    current_price,
                    invested,
                    market_value,
                    potential_p_l,
                    percentage,
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
}

/// START TRADING (opsional) → PIN (opsional) → /securities/portfolio → scrape → upsert.
pub async fn scrape_and_insert_portofolio(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    ensure_trading_session(page).await?;

    println!("Jeda 2 detik setelah PIN / mode trading siap...");
    sleep(Duration::from_secs(2)).await;

    println!("Portofolio: buka {STOCKBIT_PORTFOLIO_URL}...");
    goto_stockbit(page, STOCKBIT_PORTFOLIO_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    wait_for_portfolio_ready(page, Duration::from_secs(45)).await?;
    sleep(Duration::from_secs(1)).await;
    save_portfolio_page_screenshot(page).await?;

    let rows = scrape_portfolio_table(page).await?;
    println!("Portofolio: {} baris di-scrape.", rows.len());
    if rows.is_empty() {
        return Err("Tabel portofolio kosong / tidak terbaca".into());
    }

    let n = upsert_portofolio(session, keyspace, &rows).await?;
    println!("OK: {n} baris diinsert ke portofolio.");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::{parse_f64_number, parse_i64_number, parse_percentage};

    #[test]
    fn parse_numbers_from_portfolio_ui() {
        assert_eq!(parse_i64_number("24"), 24);
        assert_eq!(parse_f64_number("1,361.41"), 1361.41);
        assert_eq!(parse_f64_number("1,335"), 1335.0);
        assert_eq!(parse_f64_number("3,267,393"), 3_267_393.0);
        assert_eq!(parse_f64_number("-63,393"), -63_393.0);
        assert_eq!(parse_percentage("-1.94%"), -1.94);
        assert_eq!(parse_percentage("+4.72%"), 4.72);
    }
}
