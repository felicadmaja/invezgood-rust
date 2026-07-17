//! Scrape Key Stats + Corp. Action + Profile Stockbit → upsert `emiten_list`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Local, Utc};
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use rand::Rng;
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde::Deserialize;
use stockbit_browser::goto_stockbit;
use tokio::time::sleep;

const CORP_ACTION_NAV: &str = r#"[data-cy="company-navigation-corp.-action"]"#;
const PROFILE_NAV: &str = r#"[data-cy="company-navigation-profile"]"#;
const COMPANY_MORE_MENU: &str = r#"[data-cy="company-navbar-more-menu"]"#;
const UPDATE_AT_FRESH_DAYS: i64 = 30;

/// Bentuk Scylla `corporate_action`:
/// `[{"Dividend":[{"Dividend":"Rp 209"},{"Cum Date":"..."},...]}, ...]`
type CorporateAction = Vec<HashMap<String, Vec<HashMap<String, String>>>>;

/// UDT `emiten_shareholder_gt1`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct EmitenShareholderGt1 {
    pub name: String,
    #[scylla(rename = "type")]
    #[serde(rename = "type")]
    pub type_: String,
    pub location: String,
    pub domicile: String,
    pub scriples: String,
    pub scrip: String,
    pub total_shares: String,
    pub percentage: String,
}

/// UDT `emiten_shareholder`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct EmitenShareholder {
    pub name: String,
    pub value: String,
    pub shares: String,
}

/// UDT `company_profile`.
#[derive(Debug, Clone, SerializeValue, Deserialize)]
struct CompanyProfile {
    pub company_background: String,
    pub sector: String,
    pub shareholder_more_than_one_percent: Vec<EmitenShareholderGt1>,
    pub shareholders: Vec<EmitenShareholder>,
    pub ultimate_beneficial_owner: String,
}

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
    #[scylla(default_when_null)]
    emiten_icon: String,
}

#[derive(Debug, DeserializeRow)]
struct EmitenIconRow {
    #[scylla(default_when_null)]
    emiten_icon: String,
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

/// Scroll card Current Valuation ke viewport sampai terlihat.
async fn wait_scroll_current_valuation(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut scroll_y: i64 = 0;
    loop {
        let found = page
            .evaluate(format!(
                r#"(() => {{
                    window.scrollTo(0, {scroll_y});
                    const cards = Array.from(
                        document.querySelectorAll('[data-cy="card-title"]')
                    );
                    const card = cards.find((c) => {{
                        const t = c.querySelector('.ant-card-head-title p');
                        const title = t
                            ? (t.innerText || t.textContent || '').trim().toLowerCase()
                            : '';
                        return title.includes('current valuation') || title === 'valuation';
                    }});
                    if (!card) return false;
                    const rows = card.querySelectorAll('tbody tr.ant-table-row').length;
                    if (rows === 0) return false;
                    card.scrollIntoView({{ block: 'center', inline: 'nearest' }});
                    return true;
                }})()"#
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);

        if found {
            sleep(Duration::from_millis(400)).await;
            return Ok(());
        }

        scroll_y = if scroll_y >= 1800 { 0 } else { scroll_y + 400 };
        if started.elapsed() >= Duration::from_secs(30) {
            return Err("Timeout menunggu card Current Valuation".into());
        }
        sleep(Duration::from_millis(300)).await;
    }
}

/// Buka Key Stats lewat URL langsung: `/symbol/{CODE}/keystats`.
async fn open_key_stats(page: &Page, code: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://stockbit.com/symbol/{code}/keystats");
    println!("Key Stats: buka {url}");
    goto_stockbit(page, &url)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    sleep(Duration::from_millis(800)).await;
    wait_scroll_current_valuation(page).await?;
    wait_for_key_stats_cards(page).await?;
    Ok(())
}

/// Tunggu card Key Stats siap. Scroll berkala agar card lazy-load ikut ter-render.
async fn wait_for_key_stats_cards(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut scroll_y: i64 = 0;
    loop {
        // Paksa render card di bawah fold (lazy SPA).
        let _ = page
            .evaluate(format!(
                r#"(() => {{
                    window.scrollTo(0, {scroll_y});
                    const nodes = [
                        ...document.querySelectorAll('[data-cy="card-title"]'),
                        ...document.querySelectorAll('[data-cy="card-v2-recent-quarter-table"]'),
                    ];
                    for (const el of nodes) {{
                        el.scrollIntoView({{ block: 'nearest', inline: 'nearest' }});
                    }}
                    return nodes.length;
                }})()"#
            ))
            .await;
        scroll_y = if scroll_y >= 2400 { 0 } else { scroll_y + 600 };

        let status = page
            .evaluate(
                r#"(() => {
                    const cards = Array.from(
                        document.querySelectorAll('[data-cy="card-title"]')
                    );
                    const titleOf = (card) => {
                        const title = card.querySelector('.ant-card-head-title p');
                        return title
                            ? (title.innerText || title.textContent || '').trim()
                            : '';
                    };
                    const labelsOf = (root) => Array.from(
                        root.querySelectorAll('tbody tr.ant-table-row td:first-child p')
                    ).map((p) => (p.innerText || p.textContent || '').trim());
                    const hasAll = (labels, required) =>
                        required.every((r) =>
                            labels.some((l) => l === r || l.includes(r) || r.includes(l))
                        );

                    const titles = cards.map(titleOf).filter(Boolean);
                    const valuation = cards.find((c) =>
                        titleOf(c).toLowerCase().includes('valuation')
                    );
                    const profitability = cards.find((c) =>
                        titleOf(c).toLowerCase().includes('profitability')
                    );
                    const solvency = cards.find((c) =>
                        titleOf(c).toLowerCase().includes('solvency')
                    );
                    const cashFlow = cards.find((c) =>
                        titleOf(c).toLowerCase().includes('cash flow')
                    );

                    const recent = document.querySelector(
                        '[data-cy="card-v2-recent-quarter-table"]'
                    );
                    const recentLabels = recent ? labelsOf(recent) : [];

                    const valuationOk =
                        !!valuation && valuation.querySelectorAll('tbody tr').length > 0;
                    const profitOk =
                        !!profitability &&
                        hasAll(labelsOf(profitability), [
                            'Gross Profit Margin',
                            'Operating Profit Margin',
                            'Net Profit Margin',
                        ]);
                    // Income Statement tidak selalu ada di halaman Key Stats.
                    const solvencyOk =
                        !!solvency &&
                        hasAll(labelsOf(solvency), [
                            'Current Ratio',
                            'Quick Ratio',
                            'Debt to Equity',
                            'Interest Coverage',
                        ]);
                    const cashFlowOk =
                        !!cashFlow &&
                        hasAll(labelsOf(cashFlow), [
                            'Cash From Operations',
                            'Cash From Investing',
                            'Cash From Financing',
                            'Capital expenditure',
                            'Free cash flow',
                        ]);
                    const marketOk =
                        recentLabels.some((l) => l.includes('Market Cap')) &&
                        recentLabels.some((l) => l.includes('Enterprise Value')) &&
                        recentLabels.some((l) => l.includes('Current Share Outstanding')) &&
                        recentLabels.some((l) => l.includes('Free Float'));

                    const ready =
                        valuationOk && profitOk && solvencyOk && cashFlowOk && marketOk;
                    return {
                        ready,
                        titles,
                        valuationOk,
                        profitOk,
                        solvencyOk,
                        cashFlowOk,
                        marketOk,
                        url: window.location.href || '',
                    };
                })()"#,
            )
            .await?
            .into_value::<serde_json::Value>()
            .unwrap_or_else(|_| serde_json::json!({ "ready": false }));

        if status.get("ready").and_then(|v| v.as_bool()).unwrap_or(false) {
            let _ = page
                .evaluate("window.scrollTo(0, 0)")
                .await;
            return Ok(());
        }

        if started.elapsed() >= Duration::from_secs(60) {
            let detail = status.to_string();
            let shot = save_key_stats_timeout_screenshot(page, &status).await;
            let shot_info = shot
                .as_ref()
                .map(|p| format!(" | screenshot: {}", p.display()))
                .unwrap_or_default();
            return Err(format!(
                "Timeout menunggu Key Stats (Current Valuation + Profitability + Solvency + Cash Flow + Market Cap). status={detail}{shot_info}"
            )
            .into());
        }
        sleep(Duration::from_millis(350)).await;
    }
}

async fn save_key_stats_timeout_screenshot(
    page: &Page,
    status: &serde_json::Value,
) -> Option<PathBuf> {
    let code = status
        .get("url")
        .and_then(|v| v.as_str())
        .and_then(|url| {
            url.split("/symbol/")
                .nth(1)
                .and_then(|rest| rest.split('/').next())
                .map(|c| c.trim().to_uppercase())
                .filter(|c| !c.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("screenshots");
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        eprintln!("Peringatan: gagal membuat direktori screenshot: {e}");
        return None;
    }
    let path = dir.join(format!("stockbit_error_keystats_timeout_{code}.png"));
    match page
        .save_screenshot(
            ScreenshotParams::builder()
                .format(CaptureScreenshotFormat::Png)
                .build(),
            &path,
        )
        .await
    {
        Ok(_) => {
            eprintln!("Screenshot error Key Stats timeout [{code}]: {}", path.display());
            Some(path)
        }
        Err(e) => {
            eprintln!("Peringatan: gagal simpan screenshot Key Stats timeout [{code}]: {e}");
            None
        }
    }
}

/// Scroll semua card Key Stats + tabel recent-quarter ke viewport agar ter-render.
async fn scroll_key_stats_cards(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let _ = page
        .evaluate(
            r#"(() => {
                const nodes = [
                    ...document.querySelectorAll('[data-cy="card-title"]'),
                    ...document.querySelectorAll('[data-cy="card-v2-recent-quarter-table"]'),
                ];
                for (const el of nodes) {
                    el.scrollIntoView({ block: 'center', inline: 'nearest' });
                }
                window.scrollTo(0, 0);
                return nodes.length;
            })()"#,
        )
        .await;
    sleep(Duration::from_millis(300)).await;
    Ok(())
}

async fn scrape_key_stats(page: &Page) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    wait_for_key_stats_cards(page).await?;
    scroll_key_stats_cards(page).await?;
    sleep(Duration::from_millis(400)).await;

    let json = page
        .evaluate(
            r#"(() => {
                const stats = {};
                const ingestRows = (root) => {
                    if (!root) return;
                    const rows = root.querySelectorAll('tbody tr[data-row-key], tbody tr.ant-table-row');
                    for (const tr of rows) {
                        const cells = tr.querySelectorAll('td');
                        if (cells.length < 2) continue;
                        const labelEl = cells[0].querySelector('p');
                        const valueEl = cells[1].querySelector('p');
                        const label = labelEl
                            ? (labelEl.innerText || labelEl.textContent || '').trim()
                            : '';
                        const value = valueEl
                            ? (valueEl.innerText || valueEl.textContent || '').trim()
                            : '';
                        // Abaikan baris kosong / label tahun / label kuartal tabel multi-kolom.
                        if (!label || /^\d{4}$/.test(label) || /^Q[1-4]$/.test(label)) continue;
                        stats[label] = value;
                    }
                };

                // Card Key Stats (Current Valuation, Profitability, Solvency, Cash Flow, dll.).
                for (const card of document.querySelectorAll('[data-cy="card-title"]')) {
                    ingestRows(card);
                }
                // Ringkasan: Market Cap, Enterprise Value, Current Share Outstanding, Free Float.
                ingestRows(document.querySelector('[data-cy="card-v2-recent-quarter-table"]'));

                return JSON.stringify(stats);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "{}".to_string());

    let map: HashMap<String, String> = serde_json::from_str(&json)?;
    Ok(map)
}

fn has_profitability_margins(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Gross Profit Margin (Quarter)")
        && stats.contains_key("Operating Profit Margin (Quarter)")
        && stats.contains_key("Net Profit Margin (Quarter)")
}

fn has_market_cap_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Market Cap")
        && stats.contains_key("Enterprise Value")
        && stats.contains_key("Current Share Outstanding")
        && stats.contains_key("Free Float")
}

fn has_solvency_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Current Ratio (Quarter)")
        && stats.contains_key("Quick Ratio (Quarter)")
        && stats.contains_key("Debt to Equity Ratio (Quarter)")
        && stats.contains_key("LT Debt/Equity (Quarter)")
        && stats.contains_key("Total Liabilities/Equity (Quarter)")
        && stats.contains_key("Total Debt/Total Assets (Quarter)")
        && stats.contains_key("Interest Coverage (TTM)")
}

/// Cash Flow Statement (TTM) dari card Key Stats.
fn has_cash_flow_block(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Cash From Operations (TTM)")
        && stats.contains_key("Cash From Investing (TTM)")
        && stats.contains_key("Cash From Financing (TTM)")
        && stats.contains_key("Capital expenditure (TTM)")
        && stats.contains_key("Free cash flow (TTM)")
}

async fn scrape_long_name(page: &Page, emiten: &str) -> String {
    let emiten_js = serde_json::to_string(&emiten.to_ascii_uppercase()).unwrap_or_default();
    page.evaluate(format!(
        r#"((code) => {{
            // Prefer: section header ticker (h3) + nama lengkap di <h1>.
            const h3 = Array.from(document.querySelectorAll('h3')).find(
                (h) => (h.innerText || h.textContent || '').trim().toUpperCase() === code
            );
            if (h3) {{
                const section = h3.closest('section') || h3.parentElement;
                if (section) {{
                    const h1 = section.querySelector('h1');
                    const name = h1
                        ? (h1.innerText || h1.textContent || '').trim()
                        : '';
                    if (name) return name;
                }}
            }}
            // Fallback: h1 pertama yang bukan kode ticker.
            const h1s = Array.from(document.querySelectorAll('h1'))
                .map((h) => (h.innerText || h.textContent || '').trim())
                .filter((t) => t && t.toUpperCase() !== code);
            return h1s[0] || '';
        }})({emiten_js})"#
    ))
    .await
    .ok()
    .and_then(|v| v.into_value::<String>().ok())
    .unwrap_or_default()
}

async fn wait_for_corp_action_nav(
    page: &Page,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        if page.find_element(CORP_ACTION_NAV).await.is_ok() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Timeout menunggu navigasi Corp. Action".into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn click_corp_action(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    // Langsung klik bila link Corp. Action sudah terlihat di navbar.
    if page.find_element(CORP_ACTION_NAV).await.is_ok() {
        let link = page.find_element(CORP_ACTION_NAV).await?;
        link.click().await?;
        wait_for_corp_action_page(page).await?;
        return Ok(());
    }

    // Fallback: buka menu More lalu tunggu link Corp. Action muncul.
    wait_for_selector(page, COMPANY_MORE_MENU, Duration::from_secs(10)).await?;
    let more = page.find_element(COMPANY_MORE_MENU).await?;
    more.click().await?;
    wait_for_corp_action_nav(page, Duration::from_secs(10)).await?;
    let link = page.find_element(CORP_ACTION_NAV).await?;
    link.click().await?;
    wait_for_corp_action_page(page).await?;
    Ok(())
}

async fn wait_for_corp_action_page(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const wrap = document.querySelector('[data-cy="corpaction-all-wrapper"]');
                    if (!wrap) return false;
                    const url = window.location.href || '';
                    if (!url.toLowerCase().includes('/corpaction')) return false;
                    // Siap bila wrapper ada; tabel boleh kosong untuk emiten tanpa aksi.
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            // Beri waktu isi tabel dividend/rups ter-render.
            sleep(Duration::from_millis(500)).await;
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(30) {
            let url = page.url().await?.unwrap_or_default();
            return Err(format!(
                "Timeout menunggu halaman Corp. Action (URL: {url})"
            )
            .into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

/// Scrape semua blok Dividend / RUPS / Right Issue / Stock Split di Corp. Action.
async fn scrape_corporate_action(
    page: &Page,
) -> Result<CorporateAction, Box<dyn std::error::Error>> {
    let json = page
        .evaluate(
            r#"(() => {
                const wrap = document.querySelector('[data-cy="corpaction-all-wrapper"]');
                if (!wrap) return '[]';
                const out = [];
                // Setiap blok aksi adalah child langsung wrapper.
                const blocks = Array.from(wrap.children);
                for (const block of blocks) {
                    const titleEl = Array.from(block.querySelectorAll('p')).find(
                        (p) => !p.closest('table')
                    );
                    const actionType = titleEl
                        ? (titleEl.innerText || titleEl.textContent || '').trim()
                        : '';
                    if (!actionType) continue;

                    const table =
                        block.querySelector('[data-cy$="-table-wrapper"] table') ||
                        block.querySelector('table');
                    if (!table) continue;

                    const headers = Array.from(table.querySelectorAll('thead th'))
                        .map((th) => {
                            const p = th.querySelector('p');
                            return (p
                                ? (p.innerText || p.textContent || '')
                                : (th.innerText || '')
                            ).trim();
                        })
                        .filter((t) => t);
                    if (headers.length === 0) continue;

                    const rows = Array.from(
                        table.querySelectorAll('tbody tr.ant-table-row')
                    );
                    for (const tr of rows) {
                        const cells = Array.from(tr.querySelectorAll('td')).map((td) => {
                            const p = td.querySelector('p');
                            return (p ? (p.innerText || p.textContent || '') : (td.innerText || ''))
                                .trim()
                                .replace(/\s+/g, ' ');
                        });
                        if (cells.every((c) => !c)) continue;

                        const details = [];
                        for (let i = 0; i < headers.length; i++) {
                            const key = headers[i];
                            const value = cells[i] || '';
                            details.push({ [key]: value });
                        }
                        out.push({ [actionType]: details });
                    }
                }
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let items: CorporateAction = serde_json::from_str(&json)?;
    Ok(items)
}

async fn wait_for_profile_nav(
    page: &Page,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        if page.find_element(PROFILE_NAV).await.is_ok() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err("Timeout menunggu navigasi Profile".into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn click_profile(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    // Langsung klik bila link Profile sudah terlihat di navbar.
    if page.find_element(PROFILE_NAV).await.is_ok() {
        let link = page.find_element(PROFILE_NAV).await?;
        link.click().await?;
        wait_for_profile_page(page).await?;
        return Ok(());
    }

    // Fallback: buka menu More lalu tunggu link Profile muncul.
    wait_for_selector(page, COMPANY_MORE_MENU, Duration::from_secs(10)).await?;
    let more = page.find_element(COMPANY_MORE_MENU).await?;
    more.click().await?;
    wait_for_profile_nav(page, Duration::from_secs(10)).await?;
    let link = page.find_element(PROFILE_NAV).await?;
    link.click().await?;
    wait_for_profile_page(page).await?;
    Ok(())
}

async fn wait_for_profile_page(page: &Page) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let ready = page
            .evaluate(
                r#"(() => {
                    const url = (window.location.href || '').toLowerCase();
                    if (!url.includes('/profile')) return false;
                    const cards = Array.from(
                        document.querySelectorAll('[data-cy="component-background-card"]')
                    );
                    return cards.some((card) => {
                        const title = (card.innerText || '').toLowerCase();
                        return title.includes('company background');
                    });
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            sleep(Duration::from_millis(500)).await;
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(30) {
            let url = page.url().await?.unwrap_or_default();
            return Err(format!("Timeout menunggu halaman Profile (URL: {url})").into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

/// Scrape Company Background, Sector, Shareholder >1%, Shareholders, UBO.
async fn scrape_company_profile(
    page: &Page,
) -> Result<CompanyProfile, Box<dyn std::error::Error>> {
    let json = page
        .evaluate(
            r#"(() => {
                const textOf = (el) =>
                    ((el && (el.innerText || el.textContent)) || '')
                        .trim()
                        .replace(/\s+/g, ' ');

                // Company Background + Sector (index pertama di card).
                let company_background = '';
                let sector = '';
                const bgCards = Array.from(
                    document.querySelectorAll('[data-cy="component-background-card"]')
                );
                const bgCard = bgCards.find((c) =>
                    textOf(c).toLowerCase().includes('company background')
                );
                if (bgCard) {
                    const body = bgCard.querySelector('.ant-card-body');
                    const p = body && body.querySelector('p');
                    company_background = textOf(p);
                    const firstIndex = bgCard.querySelector(
                        'a[data-cy="component-link-indexes"]'
                    );
                    sector = firstIndex
                        ? textOf(firstIndex.querySelector('span') || firstIndex)
                        : '';
                }

                // Shareholder > 1%.
                const gt1 = [];
                const gt1Card = document.querySelector(
                    '[data-cy="component-shareholder-gt1-card"]'
                );
                if (gt1Card) {
                    const rows = Array.from(
                        gt1Card.querySelectorAll(
                            '.shareholder-gt1 tbody tr.ant-table-row'
                        )
                    );
                    for (const tr of rows) {
                        const cells = Array.from(tr.querySelectorAll('td')).map((td) => {
                            const a = td.querySelector('a');
                            const p = td.querySelector('p');
                            return textOf(a || p || td);
                        });
                        if (cells.length < 8 || cells.every((c) => !c)) continue;
                        gt1.push({
                            name: cells[0] || '',
                            type: cells[1] || '',
                            location: cells[2] || '',
                            domicile: cells[3] || '',
                            scriples: cells[4] || '',
                            scrip: cells[5] || '',
                            total_shares: cells[6] || '',
                            percentage: cells[7] || '',
                        });
                    }
                }

                // Shareholders (bukan "Number of Shareholders").
                const shareholders = [];
                const shCards = Array.from(
                    document.querySelectorAll('[data-cy="component-shareholder-card"]')
                );
                const shCard = shCards.find((c) => {
                    const t = textOf(c.querySelector('.ant-card-head-title')).toLowerCase();
                    return (
                        t.includes('shareholders') &&
                        !t.includes('number of shareholders')
                    );
                });
                if (shCard) {
                    const rows = Array.from(
                        shCard.querySelectorAll('tbody tr.ant-table-row')
                    );
                    for (const tr of rows) {
                        const cells = Array.from(tr.querySelectorAll('td'));
                        if (cells.length < 3) continue;
                        const nameEl =
                            cells[0].querySelector('.shareholder-name span') ||
                            cells[0].querySelector('span') ||
                            cells[0];
                        const name = textOf(nameEl);
                        const value = textOf(cells[1]);
                        const shares = textOf(cells[2]);
                        if (!name && !value && !shares) continue;
                        shareholders.push({ name, value, shares });
                    }
                }

                // Ultimate Beneficial / Beneficiary Owner.
                let ultimate_beneficial_owner = '';
                const uboTitle = Array.from(document.querySelectorAll('p')).find((p) => {
                    const t = textOf(p).toLowerCase();
                    return (
                        t.includes('ultimate beneficial') ||
                        t.includes('ultimate beneficiary')
                    );
                });
                if (uboTitle) {
                    const section =
                        uboTitle.closest('.ant-card-body') ||
                        uboTitle.parentElement ||
                        uboTitle;
                    // Tabel UBO biasanya tepat setelah judul.
                    let table = null;
                    let n = uboTitle.nextElementSibling;
                    while (n && !table) {
                        if (n.matches && n.matches('.ant-table-wrapper')) {
                            table = n;
                            break;
                        }
                        table = n.querySelector && n.querySelector('.ant-table-wrapper');
                        n = n.nextElementSibling;
                    }
                    if (!table && section) {
                        const wrappers = Array.from(
                            section.querySelectorAll('.ant-table-wrapper')
                        );
                        table = wrappers.length ? wrappers[wrappers.length - 1] : null;
                    }
                    if (table) {
                        const row = table.querySelector('tbody tr.ant-table-row td');
                        ultimate_beneficial_owner = textOf(row);
                    }
                }

                return JSON.stringify({
                    company_background,
                    sector,
                    shareholder_more_than_one_percent: gt1,
                    shareholders,
                    ultimate_beneficial_owner,
                });
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| {
            r#"{"company_background":"","sector":"","shareholder_more_than_one_percent":[],"shareholders":[],"ultimate_beneficial_owner":""}"#
                .to_string()
        });

    let profile: CompanyProfile = serde_json::from_str(&json)?;
    Ok(profile)
}

async fn upsert_emiten_list(
    session: &Session,
    keyspace: &str,
    code_name: &str,
    long_name: &str,
    emiten_icon: &str,
    key_stats: &HashMap<String, String>,
    corporate_action: &CorporateAction,
    company_profile: &CompanyProfile,
    update_at: DateTime<Utc>,
    is_konglomerasi: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_list (\
                code_name, long_name, emiten_icon, key_stats, corporate_action, company_profile, update_at, is_konglomerasi\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (
                code_name,
                long_name,
                emiten_icon,
                key_stats,
                corporate_action,
                company_profile,
                update_at,
                is_konglomerasi,
            ),
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
            "SELECT update_at, emiten_icon FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&q, (code_name,))
        .await?
        .into_rows_result()?;

    let Some(EmitenUpdateAtRow {
        update_at: Some(ts),
        emiten_icon,
    }) =
        result.maybe_first_row::<EmitenUpdateAtRow>()?
    else {
        return Ok(false);
    };

    let age = Utc::now().signed_duration_since(ts);
    Ok(age < ChronoDuration::days(UPDATE_AT_FRESH_DAYS) && !emiten_icon.trim().is_empty())
}

async fn fetch_emiten_icon_from_trending(
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let agg = format!("{}_{}", today.format("%Y-%m-%d"), code_name);
    let q = session
        .prepare(format!(
            "SELECT emiten_icon FROM {keyspace}.emiten_trending \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;
    let result = session
        .execute_unpaged(&q, (agg.as_str(),))
        .await?
        .into_rows_result()?;
    Ok(result
        .maybe_first_row::<EmitenIconRow>()?
        .map(|row| row.emiten_icon)
        .unwrap_or_default())
}

/// Returns `Ok(true)` bila diinsert, `Ok(false)` bila di-skip (update_at masih < 30 hari).
async fn scrape_one_emiten(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten: &str,
    index: usize,
    total: usize,
) -> Result<bool, Box<dyn std::error::Error>> {
    scrape_one_emiten_inner(page, session, keyspace, emiten, index, total, true).await
}

/// Scrape Key Stats + Corp. Action + Profile untuk satu `code_name` (tanpa skip fresh).
pub async fn scrape_emiten_list_for_code(
    page: &Page,
    session: &Session,
    keyspace: &str,
    code_name: &str,
) -> Result<(), String> {
    scrape_one_emiten_inner(page, session, keyspace, code_name, 1, 1, false)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn scrape_one_emiten_inner(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten: &str,
    index: usize,
    total: usize,
    skip_if_fresh: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let code = emiten.trim().to_ascii_uppercase();
    let progress = format!("{index}/{total}");

    if skip_if_fresh && is_update_at_fresh(session, keyspace, &code).await? {
        println!(
            "Key Stats: skip {code} ({progress}) — update_at belum melebihi {UPDATE_AT_FRESH_DAYS} hari ({})",
            format_elapsed(started)
        );
        return Ok(false);
    }

    println!("Key Stats: buka emiten {code} ({progress})...");
    open_key_stats(page, &code).await?;

    let key_stats = scrape_key_stats(page).await?;
    if key_stats.is_empty() {
        return Err(format!("Key Stats kosong untuk {code}").into());
    }
    if !has_profitability_margins(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Profitability \
             (Gross/Operating/Net Profit Margin Quarter)"
        )
        .into());
    }
    if !has_market_cap_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Market Cap / Enterprise Value / \
             Current Share Outstanding / Free Float"
        )
        .into());
    }
    if !has_solvency_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Solvency \
             (Current/Quick/Debt ratios + Interest Coverage)"
        )
        .into());
    }
    if !has_cash_flow_block(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Cash Flow Statement \
             (Cash From Operations/Investing/Financing, CapEx, Free cash flow TTM)"
        )
        .into());
    }

    let long_name = scrape_long_name(page, &code).await;
    let long_name = if long_name.is_empty() {
        code.as_str()
    } else {
        long_name.as_str()
    };

    println!("Corp. Action: buka halaman untuk {code}...");
    click_corp_action(page).await?;
    let corporate_action = scrape_corporate_action(page).await?;

    println!("Profile: buka halaman untuk {code}...");
    click_profile(page).await?;
    let company_profile = scrape_company_profile(page).await?;
    if company_profile.company_background.trim().is_empty() {
        return Err(format!("Profile {code}: Company Background kosong").into());
    }

    let emiten_icon = fetch_emiten_icon_from_trending(session, keyspace, &code).await?;

    upsert_emiten_list(
        session,
        keyspace,
        &code,
        long_name,
        &emiten_icon,
        &key_stats,
        &corporate_action,
        &company_profile,
        Utc::now(),
        false,
    )
    .await?;

    println!(
        "OK: emiten_list {code} ({progress}) — {} key_stats, {} corporate_action, \
         profile gt1={} shareholders={} ({})",
        key_stats.len(),
        corporate_action.len(),
        company_profile.shareholder_more_than_one_percent.len(),
        company_profile.shareholders.len(),
        format_elapsed(started)
    );
    Ok(true)
}

/// Navbar search → Key Stats → Corp. Action → Profile → upsert `emiten_list`.
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
    let total = emitens.len();
    for (i, emiten) in emitens.iter().enumerate() {
        let index = i + 1;
        let scraped = match scrape_one_emiten(page, session, keyspace, emiten, index, total).await
        {
            Ok(true) => {
                ok += 1;
                true
            }
            Ok(false) => false, // skip: data masih fresh — tanpa delay
            Err(e) => {
                eprintln!("Peringatan: emiten_list {emiten} ({index}/{total}) gagal: {e}");
                true // sudah attempt scrape — tetap delay
            }
        };
        if scraped && index < total {
            let wait_secs = rand::thread_rng().gen_range(1u64..=10);
            let wait_ms = wait_secs * 1000;
            println!(
                "Key Stats: jeda {wait_ms}ms ({wait_secs}s) sebelum emiten berikutnya..."
            );
            sleep(Duration::from_secs(wait_secs)).await;
        }
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

    #[test]
    fn profitability_margins_detected() {
        let mut map = HashMap::new();
        map.insert("Gross Profit Margin (Quarter)".into(), "35.63%".into());
        map.insert("Operating Profit Margin (Quarter)".into(), "16.85%".into());
        map.insert("Net Profit Margin (Quarter)".into(), "11.87%".into());
        assert!(has_profitability_margins(&map));
    }

    #[test]
    fn market_cap_block_detected() {
        let mut map = HashMap::new();
        map.insert("Market Cap".into(), "155,074 B".into());
        map.insert("Enterprise Value".into(), "182,005 B".into());
        map.insert("Current Share Outstanding".into(), "192.64 B".into());
        map.insert("Free Float".into(), "19.35%".into());
        assert!(has_market_cap_block(&map));
    }

    #[test]
    fn solvency_block_detected() {
        let mut map = HashMap::new();
        map.insert("Current Ratio (Quarter)".into(), "2.26".into());
        map.insert("Quick Ratio (Quarter)".into(), "2.11".into());
        map.insert("Debt to Equity Ratio (Quarter)".into(), "0.85".into());
        map.insert("LT Debt/Equity (Quarter)".into(), "0.71".into());
        map.insert("Total Liabilities/Equity (Quarter)".into(), "1.18".into());
        map.insert("Total Debt/Total Assets (Quarter)".into(), "0.35".into());
        map.insert("Interest Coverage (TTM)".into(), "4.82".into());
        assert!(has_solvency_block(&map));
    }

    #[test]
    fn cash_flow_block_detected() {
        let mut map = HashMap::new();
        map.insert("Cash From Operations (TTM)".into(), "3,294 B".into());
        map.insert("Cash From Investing (TTM)".into(), "(16,220 B)".into());
        map.insert("Cash From Financing (TTM)".into(), "5,253 B".into());
        map.insert("Capital expenditure (TTM)".into(), "(6,965 B)".into());
        map.insert("Free cash flow (TTM)".into(), "(3,671 B)".into());
        assert!(has_cash_flow_block(&map));
    }

    #[test]
    fn corporate_action_json_parses() {
        let json = r#"[
          {
            "Dividend": [
              {"Dividend": "Rp 209"},
              {"Cum Date": "20 Apr 26"},
              {"Ex Date": "21 Apr 26"},
              {"Recording Date": "22 Apr 26"},
              {"Payment Date": "8 Mei 26"}
            ]
          },
          {
            "RUPS": [
              {"Event Date": "10 Apr 26"},
              {"Time": "14:00"},
              {"Eligible Date": "10 Mar 26"},
              {"Venue": "Jakarta"}
            ]
          }
        ]"#;
        let items: CorporateAction = serde_json::from_str(json).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]["Dividend"][0].get("Dividend").map(String::as_str),
            Some("Rp 209")
        );
        assert_eq!(
            items[1]["RUPS"][0].get("Event Date").map(String::as_str),
            Some("10 Apr 26")
        );
    }

    #[test]
    fn company_profile_json_parses() {
        let json = r#"{
          "company_background": "PT Dian Swastatika Sentosa Tbk menjalankan kegiatan usaha utama.",
          "sector": "Minyak, Gas & Batu Bara",
          "shareholder_more_than_one_percent": [
            {
              "name": "SINAR MAS TUNGGAL",
              "type": "Corporate",
              "location": "Local",
              "domicile": "INDONESIA",
              "scriples": "0",
              "scrip": "115,388,080,000",
              "total_shares": "115,388,080,000",
              "percentage": "59.90%"
            }
          ],
          "shareholders": [
            {
              "name": "PT SINAR MAS TUNGGAL",
              "value": "115.39 B",
              "shares": "59.9%"
            }
          ],
          "ultimate_beneficial_owner": "FRANKY OESMAN WIDJAJA"
        }"#;
        let profile: CompanyProfile = serde_json::from_str(json).unwrap();
        assert!(profile.company_background.contains("Dian Swastatika"));
        assert_eq!(profile.sector, "Minyak, Gas & Batu Bara");
        assert_eq!(profile.shareholder_more_than_one_percent.len(), 1);
        assert_eq!(
            profile.shareholder_more_than_one_percent[0].type_,
            "Corporate"
        );
        assert_eq!(profile.shareholders[0].name, "PT SINAR MAS TUNGGAL");
        assert_eq!(
            profile.ultimate_beneficial_owner,
            "FRANKY OESMAN WIDJAJA"
        );
    }
}
