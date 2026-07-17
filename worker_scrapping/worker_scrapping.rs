//! Worker opsional — scrap Stockbit (Chrome). Tidak dijalankan oleh PM2 / binary utama.
//!
//! ```bash
//! cargo run -p worker_scrapping
//! ```
//! Buka langsung https://stockbit.com/stream.
//! Jika dialihkan ke https://stockbit.com/login (atau sesi habis), isi username/password natural,
//! lalu kembali ke /stream, scrap Top Gainer/Loser, insert Scylla.
//!
//! Env wajib saat perlu login: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`.
//! Env opsional: `CHROME_EXECUTABLE_PATH` (mis. `/usr/bin/chromium-browser`).
//! Env opsional: `STOCKBIT_2FA_TIMEOUT_SECS` (default 300 = 5 menit).
//! Env opsional: `STOCKBIT_SESSION_CHECK_SECS` (default 5) — tunggu popup sesi habis di `/stream`.
//! Env Scylla (insert `emiten_trending`, `emiten_list`, `bandarmology`): `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//!
//! Setelah movers → Top Gainer/Loser → insert `emiten_trending`.
//! Lalu MV `emiten_trending_by_tahun_bulan_tanggal` (hari ini) → Key Stats → insert `emiten_list`.
//! Kemudian Bandar Detector → Last 7D / Last 1M / Last 3M (tunggu 5 detik per period) → insert `bandarmology` (d_7, M_1, M_3).
//!
//! Profil Chrome disimpan di `worker_scrapping/browser_data/` agar cookie/sesi login tetap ada antar run.

mod bandarmology;
mod emiten_list_worker;

use chrono::Local;
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use stockbit_browser::{
    dismiss_profile_avatar_modal, goto_stockbit, launch_page, open_stream_or_login,
    STOCKBIT_STREAM_URL,
};
use tokio::time::sleep;

/// Root workspace. `CARGO_MANIFEST_DIR` = `worker_scrapping`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
}

fn screenshot_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("screenshots")
}

async fn clear_screenshot_dir() -> Result<(), Box<dyn std::error::Error>> {
    let dir = screenshot_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    let mut removed = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            tokio::fs::remove_file(entry.path()).await?;
            removed += 1;
        }
    }
    if removed > 0 {
        println!(
            "Screenshot lama dihapus: {removed} file dari {}",
            dir.display()
        );
    }
    Ok(())
}

async fn save_step_screenshot(
    page: &Page,
    step: &str,
    label: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = screenshot_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let path = dir.join(format!("stockbit_{step}_{label}.png"));
    page.save_screenshot(
        ScreenshotParams::builder()
            .format(CaptureScreenshotFormat::Png)
            .build(),
        &path,
    )
    .await?;
    println!("Screenshot [{step} {label}]: {}", path.display());
    Ok(path)
}

#[derive(Debug, Clone, Deserialize)]
struct MoversRow {
    symbol: String,
    price: String,
    price_change: String,
    value: String,
    volume: String,
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}

async fn connect_scylla() -> Result<Arc<Session>, Box<dyn std::error::Error>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri.as_str());
    if let Ok(user) = std::env::var("SCYLLA_USER") {
        if let Ok(password) = std::env::var("SCYLLA_PASSWORD") {
            builder = builder.user(user, password);
        }
    }
    Ok(Arc::new(builder.build().await?))
}

/// Normalisasi symbol: huruf saja, uppercase (contoh `kblv` → `KBLV`).
fn normalize_emiten_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// Harga saham: `"108"`, `"1,235"` → double.
fn parse_price(raw: &str) -> f64 {
    raw.trim().replace(',', "").parse().unwrap_or(0.0)
}

/// Perubahan harga: `"(+27.06%)"`, `"+27.06%"`, `"(-1.08%)"` → `27.06`, `-1.08`.
fn parse_price_change(raw: &str) -> f64 {
    let mut s = raw.trim();
    if let Some(inner) = s.strip_prefix('(').and_then(|x| x.strip_suffix(')')) {
        s = inner;
    }
    s.trim_end_matches('%').parse().unwrap_or(0.0)
}

#[cfg(test)]
mod parse_tests {
    use super::{parse_price, parse_price_change};

    #[test]
    fn parse_price_from_movers_table() {
        assert_eq!(parse_price("108"), 108.0);
        assert_eq!(parse_price("1,235"), 1235.0);
        assert_eq!(parse_price("24,775"), 24775.0);
    }

    #[test]
    fn parse_price_change_from_movers_table() {
        assert_eq!(parse_price_change("(+27.06%)"), 27.06);
        assert_eq!(parse_price_change("+27.06%"), 27.06);
        assert_eq!(parse_price_change("(-1.08%)"), -1.08);
        assert_eq!(parse_price_change("-1.08%"), -1.08);
    }
}

async fn click_mover_tab(page: &Page, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=10 {
        // Prefer id prefix MOVER_TYPE_* bila ada; fallback teks label di <p>.
        let clicked = page
            .evaluate(format!(
                r#"(() => {{
                    const label = {label_js};
                    const byId = Array.from(document.querySelectorAll('[id^="MOVER_TYPE_"]'))
                        .find((el) => {{
                            const t = (el.innerText || el.textContent || '').trim();
                            return t === label || t.includes(label);
                        }});
                    if (byId) {{ byId.click(); return true; }}
                    const nodes = Array.from(document.querySelectorAll('p, span, button, div, a'));
                    const target = nodes.find((el) => {{
                        const t = (el.innerText || el.textContent || '').trim();
                        return t === label;
                    }});
                    if (!target) return false;
                    target.click();
                    return true;
                }})()"#,
                label_js = serde_json::to_string(label).unwrap_or_else(|_| format!("\"{label}\""))
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("'{label}' diklik (attempt {attempt}).");
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(format!("Elemen '{label}' tidak ditemukan").into())
}

/// Scrape tabel movers (kolom Symbol / Price / Value / Volume).
async fn scrape_movers_table(page: &Page) -> Result<Vec<MoversRow>, Box<dyn std::error::Error>> {
    // Tunggu sampai ada baris tbody.
    for _ in 0..20 {
        let ready = page
            .evaluate(
                r#"(() => {
                    const table = document.querySelector('table');
                    if (!table) return false;
                    return table.querySelectorAll('tbody tr').length > 0;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    let json = page
        .evaluate(
            r#"(() => {
                const parsePriceCell = (cell) => {
                    if (!cell) return { price: '', price_change: '' };
                    const spans = Array.from(cell.querySelectorAll('span'));
                    if (spans.length >= 2) {
                        return {
                            price: (spans[0].innerText || '').trim(),
                            price_change: (spans[1].innerText || '').trim(),
                        };
                    }
                    const text = (cell.innerText || '').trim();
                    const m = text.match(/^([\d,.]+)\s*\(([+-]?[\d.,]+%)\)/);
                    if (m) {
                        return { price: m[1], price_change: m[2] };
                    }
                    return { price: text.split(/\s+/)[0] || '', price_change: '' };
                };

                const table = document.querySelector('table');
                if (!table) return '[]';
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                const out = rows.map((tr) => {
                    const tds = Array.from(tr.querySelectorAll('td'));
                    // Lewati kolom gap kosong: symbol, price, value, volume, ...
                    const cells = tds.filter((td) => {
                        const text = (td.innerText || '').trim();
                        return !!text;
                    });
                    const symbolEl = tr.querySelector('.symbol span[style*="font-weight"], .symbol span');
                    let symbol = symbolEl
                        ? (symbolEl.innerText || '').trim().split(/\s+/)[0]
                        : '';
                    if (!symbol && cells[0]) {
                        symbol = (cells[0].innerText || '').trim().split(/\s+/)[0];
                    }
                    const { price, price_change } = parsePriceCell(cells[1] || null);
                    const value = cells[2] ? (cells[2].innerText || '').trim() : '';
                    const volume = cells[3] ? (cells[3].innerText || '').trim() : '';
                    return { symbol, price, price_change, value, volume };
                }).filter((r) => r.symbol);
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let rows: Vec<MoversRow> = serde_json::from_str(&json)?;
    Ok(rows)
}

async fn insert_emiten_trending(
    session: &Session,
    rows: &[MoversRow],
    gainer_or_loser: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let ks = keyspace();
    let today = Local::now().date_naive();
    let date_str = today.format("%Y-%m-%d").to_string();

    let insert = session
        .prepare(format!(
            "INSERT INTO {ks}.emiten_trending (\
                agg_tahun_bulan_tanggal_emiten_name, \
                tahun_bulan_tanggal, \
                gainer_or_loser, \
                emiten_name, \
                price, \
                price_change, \
                value, \
                volume\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let mut n = 0usize;
    for row in rows {
        let emiten = normalize_emiten_name(&row.symbol);
        if emiten.is_empty() {
            continue;
        }
        let agg = format!("{date_str}_{emiten}");
        let price_change = parse_price_change(&row.price_change);
        let price = parse_price(&row.price);
        session
            .execute_unpaged(
                &insert,
                (
                    agg.as_str(),
                    today,
                    gainer_or_loser,
                    emiten.as_str(),
                    price,
                    price_change,
                    row.value.as_str(),
                    row.volume.as_str(),
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_env = workspace_root().join(".env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();

    clear_screenshot_dir().await?;

    let (mut browser, page) = launch_page().await.map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    // Awal: langsung /stream. Jika Stockbit redirect ke /login → isi username/password natural.
    open_stream_or_login(&page, &email, &password)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    // Safety: Skip lagi jika Profile Avatar muncul ulang sebelum klik movers.
    dismiss_profile_avatar_modal(&page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    println!("Mengklik right-menu-movers...");
    if let Ok(btn) = page.find_element(r#"[data-cy="right-menu-movers"]"#).await {
        btn.click().await?;
        sleep(Duration::from_secs(2)).await;
    } else {
        return Err("Error: Tombol [data-cy=\"right-menu-movers\"] tidak ditemukan!".into());
    }
    save_step_screenshot(&page, "06", "movers").await?;

    println!("Mengklik Top Gainer...");
    click_mover_tab(&page, "Top Gainer").await?;
    sleep(Duration::from_secs(2)).await;
    save_step_screenshot(&page, "07", "top_gainer").await?;

    let gainer_rows = scrape_movers_table(&page).await?;
    println!("Top Gainer: {} baris di-scrape.", gainer_rows.len());
    if gainer_rows.is_empty() {
        return Err("Tabel Top Gainer kosong / tidak terbaca".into());
    }

    let session = connect_scylla().await?;
    let inserted_gainer = insert_emiten_trending(&session, &gainer_rows, "gainer").await?;
    println!("OK: {inserted_gainer} baris diinsert ke emiten_trending (gainer).");

    println!("Mengklik Top Loser...");
    click_mover_tab(&page, "Top Loser").await?;
    sleep(Duration::from_secs(2)).await;
    let screenshot_path = save_step_screenshot(&page, "08", "top_loser").await?;

    let loser_rows = scrape_movers_table(&page).await?;
    println!("Top Loser: {} baris di-scrape.", loser_rows.len());
    if loser_rows.is_empty() {
        return Err("Tabel Top Loser kosong / tidak terbaca".into());
    }

    let inserted_loser = insert_emiten_trending(&session, &loser_rows, "loser").await?;
    println!("OK: {inserted_loser} baris diinsert ke emiten_trending (loser).");

    let today = Local::now().date_naive();
    let ks = keyspace();
    println!(
        "Query MV emiten_trending_by_tahun_bulan_tanggal untuk {}...",
        today.format("%Y-%m-%d")
    );
    let emitens = bandarmology::fetch_today_emiten_names(&session, &ks, today).await?;
    println!(
        "Ditemukan {} emiten unik hari ini (MV emiten_trending_by_tahun_bulan_tanggal).",
        emitens.len()
    );

    let key_stats_ok =
        emiten_list_worker::scrape_and_insert_key_stats(&page, &session, &ks, &emitens).await?;
    println!("OK: {key_stats_ok} emiten key_stats diinsert ke emiten_list.");

    println!("Kembali ke /stream untuk bandarmology...");
    goto_stockbit(&page, STOCKBIT_STREAM_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    sleep(Duration::from_secs(2)).await;
    dismiss_profile_avatar_modal(&page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    let bandar_ok =
        bandarmology::scrape_and_insert_bandarmology(&page, &session, &ks, today, &emitens).await?;
    println!("OK: {bandar_ok} emiten diinsert ke bandarmology.");
    save_step_screenshot(&page, "09", "bandarmology").await?;

    let final_url = page.url().await?.unwrap_or_default();
    let final_title = page.get_title().await?.unwrap_or_default();
    println!("Siap — title: {final_title:?} | url: {final_url}");

    browser.close().await?;

    println!(
        "Selesai. Screenshot terakhir: {}",
        screenshot_path.display()
    );
    Ok(())
}
