//! Scrape Key Stats Stockbit → upsert `emiten_list.key_stats`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use chromiumoxide::page::Page;
use rand::Rng;
use scylla::client::session::Session;
use scylla::DeserializeRow;
use tokio::time::sleep;

const SEARCH_INPUT: &str = r#"[data-cy="top-navbar-search-input-desktop"]"#;
const KEY_STATS_NAV: &str = r#"[data-cy="company-navigation-key-stats"]"#;
const UPDATE_AT_FRESH_DAYS: i64 = 30;

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
}

async fn type_naturally(
    page: &Page,
    selector: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let element = page
        .find_element(selector)
        .await
        .map_err(|_| format!("Elemen {selector} tidak ditemukan"))?;
    element.click().await?;
    sleep(Duration::from_millis(400)).await;

    let _ = page
        .evaluate(format!(
            r#"(() => {{
                const el = document.querySelector({selector_js});
                if (!el) return false;
                el.focus();
                el.value = '';
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                return true;
            }})()"#,
            selector_js = serde_json::to_string(selector)?
        ))
        .await;

    for ch in value.chars() {
        sleep(Duration::from_millis(rand::thread_rng().gen_range(80..220))).await;
        element.type_str(&ch.to_string()).await?;
    }
    Ok(())
}

async fn wait_for_selector(
    page: &Page,
    selector: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        if page.find_element(selector).await.is_ok() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(format!("Timeout menunggu {selector}").into());
        }
        sleep(Duration::from_millis(400)).await;
    }
}

async fn wait_for_symbol_header(
    page: &Page,
    emiten: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let want = emiten.to_ascii_uppercase();
    let want_js = serde_json::to_string(&want)?;
    let started = Instant::now();
    loop {
        // Siap bila logo company-header-icon + h3 ticker sudah tampil di halaman symbol.
        let ok = page
            .evaluate(format!(
                r#"((code) => {{
                    const want = code.toUpperCase();
                    const icon = document.querySelector('img.company-header-icon');
                    const iconOk = !!icon && (
                        (icon.getAttribute('alt') || '').trim().toUpperCase() === want ||
                        (icon.getAttribute('src') || '').toUpperCase().includes('/' + want + '.')
                    );
                    const h3ok = Array.from(document.querySelectorAll('h3')).some(
                        (h) => (h.innerText || h.textContent || '').trim().toUpperCase() === want
                    );
                    const url = window.location.href || '';
                    const urlok = url.toUpperCase().includes('/SYMBOL/' + want);
                    return iconOk && h3ok && urlok;
                }})({want_js})"#,
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ok {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            let url = page.url().await?.unwrap_or_default();
            return Err(format!(
                "Timeout menunggu halaman symbol {want} (URL: {url})"
            )
            .into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn search_emiten(page: &Page, emiten: &str) -> Result<(), Box<dyn std::error::Error>> {
    let code = emiten.trim().to_ascii_uppercase();
    wait_for_selector(page, SEARCH_INPUT, Duration::from_secs(20)).await?;
    type_naturally(page, SEARCH_INPUT, &code).await?;
    sleep(Duration::from_millis(300)).await;

    let input = page.find_element(SEARCH_INPUT).await?;
    input.press_key("Enter").await?;
    wait_for_symbol_header(page, &code, Duration::from_secs(30)).await?;
    Ok(())
}

async fn click_key_stats(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=10 {
        if let Ok(link) = page.find_element(KEY_STATS_NAV).await {
            link.click().await?;
            // Tunggu card "Current Valuation" muncul (bukan sleep hardcode).
            match wait_for_key_stats_cards(page).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let url = page.url().await?.unwrap_or_default();
                    if url.contains("/keystats") {
                        return Err(e);
                    }
                }
            }
        }
        sleep(Duration::from_millis(300)).await;
        if attempt == 10 {
            return Err("Tombol Key Stats tidak ditemukan / card Current Valuation tidak muncul".into());
        }
    }
    Ok(())
}

async fn wait_for_key_stats_cards(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const cards = Array.from(
                        document.querySelectorAll('[data-cy="card-title"]')
                    );
                    return cards.some((card) => {
                        const title = card.querySelector('.ant-card-head-title p');
                        const t = title
                            ? (title.innerText || title.textContent || '').trim()
                            : '';
                        if (t !== 'Current Valuation') return false;
                        // Pastikan baris tabel valuasi sudah ter-render.
                        return card.querySelectorAll('tbody tr').length > 0;
                    });
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(30) {
            return Err("Timeout menunggu card Key Stats (Current Valuation)".into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn scrape_key_stats(page: &Page) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    wait_for_key_stats_cards(page).await?;
    sleep(Duration::from_millis(400)).await;

    let json = page
        .evaluate(
            r#"(() => {
                const cards = Array.from(document.querySelectorAll('[data-cy="card-title"]'));
                const stats = {};
                for (const card of cards) {
                    const rows = card.querySelectorAll('tbody tr');
                    for (const tr of rows) {
                        const labelEl = tr.querySelector('td:first-child p');
                        const valueEl = tr.querySelector('td:nth-child(2) p');
                        const label = labelEl
                            ? (labelEl.innerText || labelEl.textContent || '').trim()
                            : '';
                        const value = valueEl
                            ? (valueEl.innerText || valueEl.textContent || '').trim()
                            : '';
                        if (label) {
                            stats[label] = value;
                        }
                    }
                }
                return JSON.stringify(stats);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "{}".to_string());

    let map: HashMap<String, String> = serde_json::from_str(&json)?;
    Ok(map)
}

async fn scrape_long_name(page: &Page, emiten: &str) -> String {
    let emiten_js = serde_json::to_string(&emiten.to_ascii_uppercase()).unwrap_or_default();
    page.evaluate(format!(
        r#"((code) => {{
            const h3 = Array.from(document.querySelectorAll('h3')).find(
                (h) => (h.innerText || '').trim().toUpperCase() === code
            );
            if (!h3) return '';
            let node = h3.parentElement;
            for (let i = 0; i < 4 && node; i++) {{
                node = node.parentElement;
            }}
            if (!node) return '';
            const texts = Array.from(node.querySelectorAll('p, span'))
                .map((el) => (el.innerText || el.textContent || '').trim())
                .filter((t) => t && t.toUpperCase() !== code && t.length > code.length + 2);
            return texts[0] || '';
        }})({emiten_js})"#
    ))
    .await
    .ok()
    .and_then(|v| v.into_value::<String>().ok())
    .unwrap_or_default()
}

async fn upsert_key_stats(
    session: &Session,
    keyspace: &str,
    code_name: &str,
    long_name: &str,
    key_stats: &HashMap<String, String>,
    update_at: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_list (\
                code_name, long_name, key_stats, update_at\
            ) VALUES (?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (code_name, long_name, key_stats, update_at),
        )
        .await?;
    Ok(())
}

/// `true` bila `update_at` masih < 30 hari (data masih fresh → skip scrape/insert).
async fn is_update_at_fresh(
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let q = session
        .prepare(format!(
            "SELECT update_at FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&q, (code_name,))
        .await?
        .into_rows_result()?;

    let Some(EmitenUpdateAtRow { update_at: Some(ts) }) =
        result.maybe_first_row::<EmitenUpdateAtRow>()?
    else {
        return Ok(false);
    };

    let age = Utc::now().signed_duration_since(ts);
    Ok(age < ChronoDuration::days(UPDATE_AT_FRESH_DAYS))
}

/// Returns `Ok(true)` bila diinsert, `Ok(false)` bila di-skip (update_at masih < 30 hari).
async fn scrape_one_emiten(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let code = emiten.trim().to_ascii_uppercase();

    if is_update_at_fresh(session, keyspace, &code).await? {
        println!(
            "Key Stats: skip {code} — update_at belum melebihi {UPDATE_AT_FRESH_DAYS} hari ({})",
            format_elapsed(started)
        );
        return Ok(false);
    }

    println!("Key Stats: cari emiten {code}...");
    search_emiten(page, &code).await?;
    click_key_stats(page).await?;

    let key_stats = scrape_key_stats(page).await?;
    if key_stats.is_empty() {
        return Err(format!("Key Stats kosong untuk {code}").into());
    }

    let long_name = scrape_long_name(page, &code).await;
    let long_name = if long_name.is_empty() {
        code.as_str()
    } else {
        long_name.as_str()
    };

    upsert_key_stats(
        session,
        keyspace,
        &code,
        long_name,
        &key_stats,
        Utc::now(),
    )
    .await?;

    println!(
        "OK: emiten_list {code} — {} key_stats ({})",
        key_stats.len(),
        format_elapsed(started)
    );
    Ok(true)
}

/// Navbar search → Key Stats → scrape card tables → upsert `emiten_list.key_stats`.
/// Skip emiten yang `update_at`-nya masih lebih baru dari 30 hari.
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

    let mut ok = 0usize;
    for emiten in emitens {
        match scrape_one_emiten(page, session, keyspace, emiten).await {
            Ok(true) => ok += 1,
            Ok(false) => {}
            Err(e) => eprintln!("Peringatan: key_stats {emiten} gagal: {e}"),
        }
        sleep(Duration::from_millis(500)).await;
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_stats_json_parses() {
        let json = r#"{"Current PE Ratio (TTM)":"74.08","Revenue (Quarter YoY Growth)":"-65.36%"}"#;
        let map: HashMap<String, String> = serde_json::from_str(json).unwrap();
        assert_eq!(map.get("Current PE Ratio (TTM)").map(String::as_str), Some("74.08"));
    }
}
