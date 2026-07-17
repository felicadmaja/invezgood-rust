//! Scrape Key Stats + Corp. Action Stockbit → upsert `emiten_list`.

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
const CORP_ACTION_NAV: &str = r#"[data-cy="company-navigation-corp.-action"]"#;
const COMPANY_MORE_MENU: &str = r#"[data-cy="company-navbar-more-menu"]"#;
const UPDATE_AT_FRESH_DAYS: i64 = 30;

/// Bentuk Scylla `corporate_action`:
/// `[{"Dividend":[{"Dividend":"Rp 209"},{"Cum Date":"..."},...]}, ...]`
type CorporateAction = Vec<HashMap<String, Vec<HashMap<String, String>>>>;

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
                        required.every((r) => labels.some((l) => l === r || l.includes(r)));

                    const valuation = cards.find((c) => titleOf(c) === 'Current Valuation');
                    const profitability = cards.find((c) => titleOf(c) === 'Profitability');
                    const income = cards.find((c) => titleOf(c) === 'Income Statement');
                    const solvency = cards.find((c) => titleOf(c) === 'Solvency');
                    if (!valuation || valuation.querySelectorAll('tbody tr').length === 0) {
                        return false;
                    }
                    if (!profitability || !income || !solvency) return false;

                    const profitOk = hasAll(labelsOf(profitability), [
                        'Gross Profit Margin',
                        'Operating Profit Margin',
                        'Net Profit Margin',
                    ]);
                    const incomeOk = hasAll(labelsOf(income), [
                        'Revenue (TTM)',
                        'Gross Profit (TTM)',
                        'EBITDA (TTM)',
                        'Net Income (TTM)',
                    ]);
                    const solvencyOk = hasAll(labelsOf(solvency), [
                        'Current Ratio (Quarter)',
                        'Quick Ratio (Quarter)',
                        'Debt to Equity Ratio (Quarter)',
                        'LT Debt/Equity (Quarter)',
                        'Total Liabilities/Equity (Quarter)',
                        'Total Debt/Total Assets (Quarter)',
                        'Interest Coverage (TTM)',
                    ]);
                    if (!profitOk || !incomeOk || !solvencyOk) return false;

                    // Tabel ringkasan kuartal terkini: Market Cap, Enterprise Value, dll.
                    const recent = document.querySelector('[data-cy="card-v2-recent-quarter-table"]');
                    if (!recent) return false;
                    const recentLabels = labelsOf(recent);
                    return recentLabels.includes('Market Cap')
                        && recentLabels.includes('Enterprise Value')
                        && recentLabels.includes('Current Share Outstanding')
                        && recentLabels.includes('Free Float');
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(45) {
            return Err(
                "Timeout menunggu Key Stats (Valuation + Profitability + Income Statement + Solvency + Market Cap)"
                    .into(),
            );
        }
        sleep(Duration::from_millis(200)).await;
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

                // Card Key Stats (Current Valuation, Profitability, dll.).
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

fn has_income_statement_ttm(stats: &HashMap<String, String>) -> bool {
    stats.contains_key("Revenue (TTM)")
        && stats.contains_key("Gross Profit (TTM)")
        && stats.contains_key("EBITDA (TTM)")
        && stats.contains_key("Net Income (TTM)")
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

async fn upsert_emiten_list(
    session: &Session,
    keyspace: &str,
    code_name: &str,
    long_name: &str,
    key_stats: &HashMap<String, String>,
    corporate_action: &CorporateAction,
    update_at: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_list (\
                code_name, long_name, key_stats, corporate_action, update_at\
            ) VALUES (?, ?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (
                code_name,
                long_name,
                key_stats,
                corporate_action,
                update_at,
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
    if !has_income_statement_ttm(&key_stats) {
        return Err(format!(
            "Key Stats {code} belum berisi Income Statement \
             (Revenue/Gross Profit/EBITDA/Net Income TTM)"
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

    let long_name = scrape_long_name(page, &code).await;
    let long_name = if long_name.is_empty() {
        code.as_str()
    } else {
        long_name.as_str()
    };

    println!("Corp. Action: buka halaman untuk {code}...");
    click_corp_action(page).await?;
    let corporate_action = scrape_corporate_action(page).await?;

    upsert_emiten_list(
        session,
        keyspace,
        &code,
        long_name,
        &key_stats,
        &corporate_action,
        Utc::now(),
    )
    .await?;

    println!(
        "OK: emiten_list {code} — {} key_stats, {} corporate_action ({})",
        key_stats.len(),
        corporate_action.len(),
        format_elapsed(started)
    );
    Ok(true)
}

/// Navbar search → Key Stats → Corp. Action → upsert `emiten_list`.
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
            Err(e) => eprintln!("Peringatan: emiten_list {emiten} gagal: {e}"),
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
    fn income_statement_ttm_detected() {
        let mut map = HashMap::new();
        map.insert("Revenue (TTM)".into(), "45,589 B".into());
        map.insert("Gross Profit (TTM)".into(), "14,952 B".into());
        map.insert("EBITDA (TTM)".into(), "7,486 B".into());
        map.insert("Net Income (TTM)".into(), "3,855 B".into());
        assert!(has_income_statement_ttm(&map));
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
}
