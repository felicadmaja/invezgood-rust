//! Scrape Top Gainer / Top Loser (movers) Stockbit → insert `emiten_trending`.

use chrono::Local;
use chromiumoxide::page::Page;
use gcs::{download_and_upload_emiten_icon, GcsOAuthTokenCache, GcsSignedUrlRuntime};
use scylla::client::session::Session;
use serde::Deserialize;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Deserialize)]
struct MoversRow {
    symbol: String,
    emiten_icon: String,
    price: String,
    price_change: String,
    value: String,
    volume: String,
    freq: String,
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

/// `emiten_trending.price_change` (double).
/// Contoh UI: `"(+26.85%)"` → `26.85`, `"(-1.08%)"` → `-1.08`.
/// Buang `(`, `)`, `%`, spasi; tanda `+`/`-` tetap (positif tanpa `+`).
fn parse_price_change(raw: &str) -> f64 {
    let cleaned: String = raw
        .chars()
        .filter(|c| *c != '(' && *c != ')' && *c != '%' && !c.is_whitespace())
        .collect();
    cleaned.parse().unwrap_or(0.0)
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

/// Download icon dari Stockbit assets lalu upload ke GCS (`stoksaham/icon/{CODE}.ext`).
/// Path object GCS disimpan ke DB (bukan path lokal).
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

/// Scrape tabel movers di `#movers-table-wrapper` (Symbol / Price / Value / Volume).
async fn scrape_movers_table(page: &Page) -> Result<Vec<MoversRow>, Box<dyn std::error::Error>> {
    // Tunggu sampai ada baris tbody di tabel movers.
    for _ in 0..20 {
        let ready = page
            .evaluate(
                r#"(() => {
                    const table = document.querySelector('#movers-table-wrapper table');
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
                    // Contoh: <span>137</span><span class="green">(+26.85%)</span>
                    if (spans.length >= 2) {
                        return {
                            price: (spans[0].innerText || '').trim(),
                            price_change: (spans[1].innerText || '').trim(),
                        };
                    }
                    const text = (cell.innerText || '').trim();
                    const m = text.match(/^([\d,.]+)\s*\(([+-]?[\d.,]+%)\)/);
                    if (m) {
                        return { price: m[1], price_change: '(' + m[2] + ')' };
                    }
                    return { price: text.split(/\s+/)[0] || '', price_change: '' };
                };

                const table = document.querySelector('#movers-table-wrapper table');
                if (!table) return '[]';
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                const out = rows.map((tr) => {
                    const tds = Array.from(tr.querySelectorAll('td'));
                    // Lewati kolom gap kosong: symbol, price, value, volume, freq, ...
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
                    const iconEl = tr.querySelector(
                        '.symbol img, td:first-child img, img'
                    );
                    const emiten_icon = iconEl
                        ? (iconEl.currentSrc || iconEl.getAttribute('src') || '').trim()
                        : '';
                    const { price, price_change } = parsePriceCell(cells[1] || null);
                    const value = cells[2] ? (cells[2].innerText || '').trim() : '';
                    const volume = cells[3] ? (cells[3].innerText || '').trim() : '';
                    const freq = cells[4] ? (cells[4].innerText || '').trim() : '';
                    return { symbol, emiten_icon, price, price_change, value, volume, freq };
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
    keyspace: &str,
    rows: &[MoversRow],
    gainer_or_loser: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = today.format("%Y-%m-%d").to_string();

    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_trending (\
                agg_tahun_bulan_tanggal_emiten_name, \
                tahun_bulan_tanggal, \
                gainer_or_loser, \
                emiten_name, \
                emiten_icon, \
                price, \
                price_change, \
                value, \
                volume, \
                freq, \
                updated_at\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))"
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
        let emiten_icon = match upload_emiten_icon_to_gcs(&emiten, &row.emiten_icon).await {
            Ok(path) => path,
            Err(e) => {
                eprintln!("Peringatan: gagal upload icon GCS {emiten}: {e}");
                String::new()
            }
        };
        session
            .execute_unpaged(
                &insert,
                (
                    agg.as_str(),
                    today,
                    gainer_or_loser,
                    emiten.as_str(),
                    emiten_icon.as_str(),
                    price,
                    price_change,
                    row.value.as_str(),
                    row.volume.as_str(),
                    row.freq.as_str(),
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
}

/// Klik movers → Top Gainer + Top Loser → insert `emiten_trending`.
/// Returns `(inserted_gainer, inserted_loser)`.
pub async fn scrape_and_insert_movers(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    println!("Mengklik right-menu-movers...");
    if let Ok(btn) = page.find_element(r#"[data-cy="right-menu-movers"]"#).await {
        btn.click().await?;
        sleep(Duration::from_secs(2)).await;
    } else {
        return Err("Error: Tombol [data-cy=\"right-menu-movers\"] tidak ditemukan!".into());
    }

    println!("Mengklik Top Gainer...");
    click_mover_tab(page, "Top Gainer").await?;
    sleep(Duration::from_secs(2)).await;

    let gainer_rows = scrape_movers_table(page).await?;
    println!("Top Gainer: {} baris di-scrape.", gainer_rows.len());
    if gainer_rows.is_empty() {
        return Err("Tabel Top Gainer kosong / tidak terbaca".into());
    }

    let inserted_gainer =
        insert_emiten_trending(session, keyspace, &gainer_rows, "gainer").await?;
    println!("OK: {inserted_gainer} baris diinsert ke emiten_trending (gainer).");

    println!("Mengklik Top Loser...");
    click_mover_tab(page, "Top Loser").await?;
    sleep(Duration::from_secs(2)).await;

    let loser_rows = scrape_movers_table(page).await?;
    println!("Top Loser: {} baris di-scrape.", loser_rows.len());
    if loser_rows.is_empty() {
        return Err("Tabel Top Loser kosong / tidak terbaca".into());
    }

    let inserted_loser = insert_emiten_trending(session, keyspace, &loser_rows, "loser").await?;
    println!("OK: {inserted_loser} baris diinsert ke emiten_trending (loser).");

    Ok((inserted_gainer, inserted_loser))
}

#[cfg(test)]
mod tests {
    use super::{parse_price, parse_price_change};

    #[test]
    fn parse_price_from_movers_table() {
        assert_eq!(parse_price("108"), 108.0);
        assert_eq!(parse_price("1,235"), 1235.0);
        assert_eq!(parse_price("24,775"), 24775.0);
    }

    #[test]
    fn parse_price_change_from_movers_table() {
        // Contoh UI Top Gainer: <span class="green">(+26.85%)</span>
        assert_eq!(parse_price_change("(+26.85%)"), 26.85);
        assert_eq!(parse_price_change("(+27.06%)"), 27.06);
        assert_eq!(parse_price_change("+27.06%"), 27.06);
        // Top Loser / nilai turun → negatif
        assert_eq!(parse_price_change("(-1.08%)"), -1.08);
        assert_eq!(parse_price_change("-1.08%"), -1.08);
        assert_eq!(parse_price_change("(-12.50%)"), -12.50);
    }
}
