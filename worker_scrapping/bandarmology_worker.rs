//! Scrape Bandar Detector → insert `bandarmology` (kolom d_7, M_1, M_3, dan M_12).

use chrono::NaiveDate;
use chromiumoxide::page::Page;
use rand::Rng;
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;

const PERIODS: &[(&str, &str)] = &[
    ("Last 7D", "d_7"),
    ("Last 1M", "M_1"),
    ("Last 3M", "M_3"),
    ("Last 1Y", "M_12"),
];

/// Rentang jeda acak setelah klik tombol period sebelum scrape tabel.
const PERIOD_SCRAPE_WAIT_MIN_MS: u64 = 800;
const PERIOD_SCRAPE_WAIT_MAX_MS: u64 = 2000;

fn format_wait_ms(ms: u64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyTopStats {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    pub acc_dist: String,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyBrokerBuy {
    pub broker_code: String,
    pub buy_volume: String,
    pub buy_lot: String,
    pub buy_avg: i64,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyBrokerSell {
    pub broker_code: String,
    pub sell_volume: String,
    pub sell_lot: String,
    pub sell_avg: i64,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyDay {
    pub top_1: BandarmologyTopStats,
    pub top_3: BandarmologyTopStats,
    pub top_5: BandarmologyTopStats,
    pub average: BandarmologyTopStats,
    pub net_volume: i64,
    pub net_value: String,
    pub average_rp: i64,
    pub broker_buy: Vec<BandarmologyBrokerBuy>,
    pub broker_sell: Vec<BandarmologyBrokerSell>,
}

#[derive(Debug, DeserializeRow)]
struct AggRow {
    agg_tahun_bulan_tanggal_emiten_name: String,
}

#[derive(Debug, DeserializeRow)]
struct EmitenNameRow {
    emiten_name: String,
}

fn empty_top() -> BandarmologyTopStats {
    BandarmologyTopStats {
        volume: 0,
        percent: 0.0,
        rp_b: 0,
        acc_dist: String::new(),
    }
}

fn empty_day() -> BandarmologyDay {
    BandarmologyDay {
        top_1: empty_top(),
        top_3: empty_top(),
        top_5: empty_top(),
        average: empty_top(),
        net_volume: 0,
        net_value: String::new(),
        average_rp: 0,
        broker_buy: Vec::new(),
        broker_sell: Vec::new(),
    }
}

/// Ambil daftar `emiten_name` unik untuk tanggal hari ini via MV + base table.
pub async fn fetch_today_emiten_names(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mv_q = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {keyspace}.emiten_trending_by_tahun_bulan_tanggal \
             WHERE tahun_bulan_tanggal = ?"
        ))
        .await?;
    let base_q = session
        .prepare(format!(
            "SELECT emiten_name FROM {keyspace}.emiten_trending \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;

    let mv_rows = session
        .execute_unpaged(&mv_q, (today,))
        .await?
        .into_rows_result()?;

    let mut names = BTreeSet::new();
    for row in mv_rows.rows::<AggRow>()? {
        let agg = row?.agg_tahun_bulan_tanggal_emiten_name;
        let base = session
            .execute_unpaged(&base_q, (agg.as_str(),))
            .await?
            .into_rows_result()?;
        if let Some(EmitenNameRow { emiten_name }) = base.maybe_first_row::<EmitenNameRow>()? {
            let n = emiten_name.trim().to_ascii_uppercase();
            if !n.is_empty() {
                names.insert(n);
            }
        }
    }
    Ok(names.into_iter().collect())
}

async fn click_bandar_menu(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=15 {
        let clicked = page
            .evaluate(
                r#"(() => {
                    const btn = document.querySelector('[data-cy="right-menu-bandar_detector"]');
                    if (!btn) return false;
                    btn.click();
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("Bandar Detector diklik (attempt {attempt}).");
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Tombol [data-cy=\"right-menu-bandar_detector\"] tidak ditemukan".into())
}

async fn wait_for_company_search(
    page: &Page,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const input = document.querySelector(
                        'input.ant-input[placeholder="Type here to add companies ..."]'
                    );
                    return !!input;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Input 'Type here to add companies ...' tidak muncul".into());
        }
        sleep(Duration::from_millis(400)).await;
    }
}

async fn clear_selected_companies(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let _ = page
        .evaluate(
            r#"(() => {
                document
                    .querySelectorAll(
                        '.ant-select-selection-item-remove, .anticon-close, [aria-label="Remove"]'
                    )
                    .forEach((el) => {
                        try { el.click(); } catch (_) {}
                    });
                return true;
            })()"#,
        )
        .await;
    sleep(Duration::from_millis(500)).await;
    Ok(())
}

async fn add_company(page: &Page, emiten: &str) -> Result<(), Box<dyn std::error::Error>> {
    clear_selected_companies(page).await?;
    wait_for_company_search(page, Duration::from_secs(20)).await?;

    let selector = r#"input.ant-input[placeholder="Type here to add companies ..."]"#;
    let element = page.find_element(selector).await?;
    element.click().await?;
    sleep(Duration::from_millis(300)).await;

    // Clear existing value via select-all + backspace (headless-safe).
    let _ = page
        .evaluate(
            r#"(() => {
                const input = document.querySelector(
                    'input.ant-input[placeholder="Type here to add companies ..."]'
                );
                if (!input) return false;
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', { bubbles: true }));
                return true;
            })()"#,
        )
        .await;

    for ch in emiten.chars() {
        let delay = rand::thread_rng().gen_range(100u64..=400);
        sleep(Duration::from_millis(delay)).await;
        element.type_str(&ch.to_string()).await?;
    }
    let enter_wait_ms = rand::thread_rng().gen_range(300u64..=800);
    println!(
        "Emiten {emiten} diketik (natural 100–400ms/karakter); jeda {enter_wait_ms} ms lalu Enter..."
    );
    sleep(Duration::from_millis(enter_wait_ms)).await;
    element.press_key("Enter").await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn open_datepicker(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    // Setelah period multi-hari (Last 7D / 1M / …) label tombol sering jadi range
    // "10 Jul 26 - 16 Jul 26", bukan single "16 Jul 26".
    for attempt in 1..=20 {
        let clicked = page
            .evaluate(
                r#"(() => {
                    const dateRe = /\d{1,2}\s+[A-Za-z]{3}\s+\d{2}/;
                    const rangeRe = /^\d{1,2}\s+[A-Za-z]{3}\s+\d{2}\s*[-–—]\s*\d{1,2}\s+[A-Za-z]{3}\s+\d{2}$/;
                    const singleRe = /^\d{1,2}\s+[A-Za-z]{3}\s+\d{2}$/;

                    const candidates = Array.from(
                        document.querySelectorAll('button, [role="button"], .ant-btn')
                    );
                    const target = candidates.find((b) => {
                        const t = (b.innerText || b.textContent || '')
                            .replace(/\s+/g, ' ')
                            .trim();
                        if (!t) return false;
                        return singleRe.test(t) || rangeRe.test(t) || (
                            dateRe.test(t) && t.length <= 40 && !t.includes('\n')
                        );
                    });
                    if (!target) return false;
                    target.scrollIntoView({ block: 'center', inline: 'nearest' });
                    target.click();
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            // Tunggu popover datepicker.
            for _ in 0..24 {
                let open = page
                    .evaluate(
                        r#"(() => !!document.querySelector(
                            '.react-datepicker, .ant-popover .react-datepicker, .ant-picker-dropdown'
                        ))()"#,
                    )
                    .await?
                    .into_value::<bool>()
                    .unwrap_or(false);
                if open {
                    println!("Datepicker terbuka (attempt {attempt}).");
                    return Ok(());
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    // Debug: tampilkan teks tombol yang mengandung pola tanggal (jika ada).
    let debug = page
        .evaluate(
            r#"(() => {
                const dateRe = /\d{1,2}\s+[A-Za-z]{3}\s+\d{2}/;
                return Array.from(document.querySelectorAll('button, [role="button"], .ant-btn'))
                    .map((b) => (b.innerText || b.textContent || '').replace(/\s+/g, ' ').trim())
                    .filter((t) => dateRe.test(t))
                    .slice(0, 8);
            })()"#,
        )
        .await
        .ok()
        .and_then(|v| v.into_value::<Vec<String>>().ok())
        .unwrap_or_default();
    if !debug.is_empty() {
        eprintln!("Debug tombol bertanggal (tidak diklik): {debug:?}");
    }

    Err("Tombol tanggal / datepicker tidak muncul".into())
}

async fn click_period_button(
    page: &Page,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let label_js = serde_json::to_string(label)?;
    for attempt in 1..=10 {
        let clicked = page
            .evaluate(format!(
                r#"(() => {{
                    const label = {label_js};
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const target = buttons.find((b) => {{
                        const t = (b.innerText || b.textContent || '').trim();
                        return t === label;
                    }});
                    if (!target) return false;
                    target.click();
                    return true;
                }})()"#
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("Period '{label}' diklik (attempt {attempt}).");
            // Tunggu trigger tanggal kembali (format single ATAU range).
            for _ in 0..20 {
                let ready = page
                    .evaluate(
                        r#"(() => {
                            const dateRe = /\d{1,2}\s+[A-Za-z]{3}\s+\d{2}/;
                            return Array.from(
                                document.querySelectorAll('button, [role="button"], .ant-btn')
                            ).some((b) => {
                                const t = (b.innerText || b.textContent || '')
                                    .replace(/\s+/g, ' ')
                                    .trim();
                                return dateRe.test(t) && t.length <= 40;
                            });
                        })()"#,
                    )
                    .await?
                    .into_value::<bool>()
                    .unwrap_or(false);
                if ready {
                    break;
                }
                sleep(Duration::from_millis(300)).await;
            }
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err(format!("Tombol period '{label}' tidak ditemukan").into())
}

async fn wait_for_bandar_tables(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..30 {
        let ready = page
            .evaluate(
                r#"(() => {
                    const top1 = document.querySelector('tr[data-row-key="top1"]');
                    const byRow = document.querySelector(
                        '.sc-80e8ce32-26 tr[data-row-key="0"], .scrollable tr[data-row-key="0"]'
                    );
                    return !!(top1 && (byRow || document.body.innerText.includes('Net Volume')));
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Tabel bandarmology (Top 1 / broker) tidak siap".into())
}

async fn scrape_bandar_day(page: &Page) -> Result<BandarmologyDay, Box<dyn std::error::Error>> {
    wait_for_bandar_tables(page).await?;
    let json = page
        .evaluate(
            r#"(() => {
                const parseIntClean = (s) => {
                    if (s == null) return 0;
                    const t = String(s).replace(/,/g, '').trim();
                    if (!t || t === '-' || t === '—') return 0;
                    const n = parseInt(t, 10);
                    return Number.isFinite(n) ? n : 0;
                };
                const parseFloatClean = (s) => {
                    if (s == null) return 0;
                    const t = String(s).replace(/,/g, '').trim();
                    if (!t || t === '-' || t === '—') return 0;
                    const n = parseFloat(t);
                    return Number.isFinite(n) ? n : 0;
                };
                // rp_b UI sering desimal (mis. -0.9) → simpan ×1000 sebagai bigint
                const parseRpB = (s) => Math.round(parseFloatClean(s) * 1000);

                const cellText = (td) => {
                    if (!td) return '';
                    const p = td.querySelector('p');
                    return ((p && p.innerText) || td.innerText || '').trim();
                };

                const topRow = (key) => {
                    const tr = document.querySelector('tr[data-row-key="' + key + '"]');
                    if (!tr) {
                        return { volume: 0, percent: 0, rp_b: 0, acc_dist: '' };
                    }
                    const tds = Array.from(tr.querySelectorAll('td'));
                    return {
                        volume: parseIntClean(cellText(tds[1])),
                        percent: parseFloatClean(cellText(tds[2])),
                        rp_b: parseRpB(cellText(tds[3])),
                        acc_dist: cellText(tds[4]),
                    };
                };

                const summaryVal = (label) => {
                    const rows = Array.from(document.querySelectorAll('tr[data-row-key^="summary"]'));
                    for (const tr of rows) {
                        const tds = Array.from(tr.querySelectorAll('td'));
                        if (cellText(tds[0]) === label) return cellText(tds[1]);
                    }
                    // fallback scan
                    const all = Array.from(document.querySelectorAll('tr'));
                    for (const tr of all) {
                        const tds = Array.from(tr.querySelectorAll('td'));
                        if (cellText(tds[0]) === label) return cellText(tds[1]);
                    }
                    return '';
                };

                const brokerRows = [];
                const body =
                    document.querySelector('.sc-80e8ce32-27 .ant-table-body tbody') ||
                    document.querySelector('.scrollable .ant-table-body tbody');
                if (body) {
                    Array.from(body.querySelectorAll('tr[data-row-key]')).forEach((tr) => {
                        if (tr.getAttribute('aria-hidden') === 'true') return;
                        const tds = Array.from(tr.querySelectorAll('td'));
                        if (tds.length < 8) return;
                        const buyCode = cellText(tds[0]);
                        const sellCode = cellText(tds[4]);
                        if (!buyCode && !sellCode) return;
                        brokerRows.push({
                            buy: {
                                broker_code: buyCode,
                                buy_volume: cellText(tds[1]),
                                buy_lot: cellText(tds[2]),
                                buy_avg: parseIntClean(cellText(tds[3])),
                            },
                            sell: {
                                broker_code: sellCode,
                                sell_volume: cellText(tds[5]),
                                sell_lot: cellText(tds[6]),
                                sell_avg: parseIntClean(cellText(tds[7])),
                            },
                        });
                    });
                }

                const out = {
                    top_1: topRow('top1'),
                    top_3: topRow('top3'),
                    top_5: topRow('top5'),
                    average: topRow('average'),
                    net_volume: parseIntClean(summaryVal('Net Volume')),
                    net_value: summaryVal('Net Value'),
                    average_rp: parseIntClean(summaryVal('Average (Rp)')),
                    broker_buy: brokerRows
                        .filter((r) => r.buy.broker_code)
                        .map((r) => r.buy),
                    broker_sell: brokerRows
                        .filter((r) => r.sell.broker_code)
                        .map((r) => r.sell),
                };
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "{}".to_string());

    match serde_json::from_str::<BandarmologyDay>(&json) {
        Ok(day) => Ok(day),
        Err(e) => {
            eprintln!("Peringatan: parse bandar day gagal ({e}); json={json}");
            Ok(empty_day())
        }
    }
}

async fn scrape_period(
    page: &Page,
    period_label: &str,
    col: &str,
) -> Result<BandarmologyDay, Box<dyn std::error::Error>> {
    open_datepicker(page).await?;
    click_period_button(page, period_label).await?;
    let wait_ms =
        rand::thread_rng().gen_range(PERIOD_SCRAPE_WAIT_MIN_MS..=PERIOD_SCRAPE_WAIT_MAX_MS);
    println!(
        "  {col} ({period_label}): tunggu {} sebelum scrape...",
        format_wait_ms(wait_ms)
    );
    sleep(Duration::from_millis(wait_ms)).await;
    scrape_bandar_day(page).await
}

fn bandarmology_agg(today: NaiveDate, emiten: &str) -> String {
    format!("{}_{emiten}", today.format("%Y-%m-%d"))
}

/// Kunci partition bandarmology hari ini untuk emiten, mis. `2026-07-17_BBCA`.
pub fn bandarmology_agg_key(today: NaiveDate, emiten: &str) -> String {
    bandarmology_agg(today, emiten.trim().to_ascii_uppercase().as_str())
}

/// `true` bila baris `bandarmology` untuk agg hari ini + emiten sudah ada.
pub async fn bandarmology_exists_for_today(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<bool, String> {
    let agg = bandarmology_agg_key(today, emiten);
    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await
        .map_err(|e| e.to_string())?;
    bandarmology_exists(session, &exists_stmt, &agg)
        .await
        .map_err(|e| e.to_string())
}

async fn scrape_bandarmology_days_for_emiten(
    page: &Page,
    emiten: &str,
) -> Result<Vec<BandarmologyDay>, Box<dyn std::error::Error>> {
    add_company(page, emiten).await?;

    let mut days: Vec<BandarmologyDay> = Vec::with_capacity(PERIODS.len());
    for (period_label, col) in PERIODS {
        match scrape_period(page, period_label, col).await {
            Ok(day) => {
                println!(
                    "  {col} ({period_label}): net_volume={} brokers_buy={}",
                    day.net_volume,
                    day.broker_buy.len()
                );
                days.push(day);
            }
            Err(e) => {
                eprintln!("  Gagal scrape {col} ({period_label}) untuk {emiten}: {e}");
                days.push(empty_day());
            }
        }
    }
    while days.len() < PERIODS.len() {
        days.push(empty_day());
    }
    Ok(days)
}

/// Scrape Bandar Detector untuk satu emiten bila agg hari ini belum ada.
/// Returns `Ok(true)` bila di-scrape+insert, `Ok(false)` bila sudah ada.
pub async fn scrape_bandarmology_for_code_if_missing(
    page: &Page,
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<bool, String> {
    let code = emiten.trim().to_ascii_uppercase();
    if bandarmology_exists_for_today(session, keyspace, today, &code).await? {
        let agg = bandarmology_agg_key(today, &code);
        println!("Skip {code}: bandarmology sudah ada (agg={agg}).");
        return Ok(false);
    }

    click_bandar_menu(page).await.map_err(|e| e.to_string())?;
    sleep(Duration::from_secs(2)).await;
    wait_for_company_search(page, Duration::from_secs(30))
        .await
        .map_err(|e| e.to_string())?;

    println!("\n=== Bandarmology on-demand emiten={code} ===");
    let days = scrape_bandarmology_days_for_emiten(page, &code)
        .await
        .map_err(|e| e.to_string())?;
    insert_bandarmology(
        session,
        keyspace,
        today,
        &code,
        &days[0],
        &days[1],
        &days[2],
        &days[3],
    )
    .await
    .map_err(|e| e.to_string())?;
    println!("OK: bandarmology insert {code} (on-demand).");
    Ok(true)
}

async fn bandarmology_exists(
    session: &Session,
    exists_stmt: &scylla::statement::prepared::PreparedStatement,
    agg: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let result = session
        .execute_unpaged(exists_stmt, (agg,))
        .await?
        .into_rows_result()?;
    Ok(result.rows_num() > 0)
}

async fn insert_bandarmology(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
    d_7: &BandarmologyDay,
    m_1: &BandarmologyDay,
    m_3: &BandarmologyDay,
    m_12: &BandarmologyDay,
) -> Result<(), Box<dyn std::error::Error>> {
    let agg = bandarmology_agg(today, emiten);
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.bandarmology (\
                agg_tahun_bulan_tanggal_emiten_name, \
                emiten_name, \
                tahun_bulan_tanggal, \
                d_7, \"M_1\", \"M_3\", \"M_12\"\
            ) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (agg.as_str(), emiten, today, d_7, m_1, m_3, m_12),
        )
        .await?;
    Ok(())
}

/// Buka Bandar Detector, scrape period Last 7D + Last 1M + Last 3M + Last 1Y untuk setiap emiten, insert Scylla.
pub async fn scrape_and_insert_bandarmology(
    page: &Page,
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emitens: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if emitens.is_empty() {
        println!("Tidak ada emiten untuk bandarmology.");
        return Ok(0);
    }

    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;

    // Filter dulu: skip yang sudah ada di PK agar tidak buka UI sia-sia.
    let mut todo: Vec<String> = Vec::new();
    let mut skipped = 0usize;
    for emiten in emitens {
        let agg = bandarmology_agg(today, emiten);
        if bandarmology_exists(session, &exists_stmt, &agg).await? {
            println!("Skip {emiten}: bandarmology sudah ada (agg={agg}).");
            skipped += 1;
        } else {
            todo.push(emiten.clone());
        }
    }
    println!(
        "Bandarmology: {} perlu scrape, {} sudah ada (skip).",
        todo.len(),
        skipped
    );
    if todo.is_empty() {
        return Ok(0);
    }

    click_bandar_menu(page).await?;
    sleep(Duration::from_secs(2)).await;
    wait_for_company_search(page, Duration::from_secs(30)).await?;

    let mut ok = 0usize;
    for (idx, emiten) in todo.iter().enumerate() {
        println!(
            "\n=== Bandarmology [{}/{}] emiten={} ===",
            idx + 1,
            todo.len(),
            emiten
        );

        let days = match scrape_bandarmology_days_for_emiten(page, emiten).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Skip {emiten}: gagal add company / scrape ({e})");
                continue;
            }
        };

        if let Err(e) = insert_bandarmology(
            session,
            keyspace,
            today,
            emiten,
            &days[0],
            &days[1],
            &days[2],
            &days[3],
        )
        .await
        {
            eprintln!("Gagal insert bandarmology {emiten}: {e}");
        } else {
            ok += 1;
            println!("OK: bandarmology insert {emiten}");
        }

        if idx + 1 < todo.len() {
            let wait_ms = rand::thread_rng().gen_range(2000u64..=5000);
            println!(
                "Jeda {} sebelum emiten berikutnya...",
                format_wait_ms(wait_ms)
            );
            sleep(Duration::from_millis(wait_ms)).await;
        }
    }
    Ok(ok)
}
