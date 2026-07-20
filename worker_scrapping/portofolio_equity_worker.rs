//! DOM scrape header equity di https://stockbit.com/securities/portfolio
//! → upsert `portofolio_equity`.
//!
//! Dipanggil setelah PIN/trading session siap, **sebelum** GET portfolio API
//! dan order API. Mapping:
//! - Trading Balance → `nama="Trading Balance"`
//! - Invested → `nama="Invested"`
//! - Open → `nama="Open"`
//! - Net Profit / Loss (angka sebelum `%`, contoh `-475,053 (-0.4%)` → `-475053`)
//!   → `nama="Net Profit Loss"`
//! - Total Equity → `nama="Total Equity"`

use chromiumoxide::page::Page;
use scylla::client::session::Session;
use serde::Deserialize;
use std::time::{Duration, Instant};
use stockbit_browser::goto_stockbit;
use tokio::time::sleep;

const STOCKBIT_PORTFOLIO_URL: &str = "https://stockbit.com/securities/portfolio";
const HEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const HEADER_POLL: Duration = Duration::from_millis(400);

#[derive(Debug, Clone, Deserialize)]
struct EquityDomRaw {
    trading_balance: Option<String>,
    invested: Option<String>,
    open: Option<String>,
    net_profit_loss: Option<String>,
    total_equity: Option<String>,
}

/// Parse angka equity dari teks DOM: buang pemisah ribuan; untuk Net P/L
/// ambil bagian sebelum `(` saja.
///
/// Contoh: `"24,122,660"` → `24122660.0`, `"-475,053 (-0.4%)"` → `-475053.0`.
pub fn parse_equity_number(raw: &str) -> Result<f64, String> {
    let main = raw.split('(').next().unwrap_or(raw).trim();
    let cleaned: String = main
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-' || *c == '.')
        .collect();
    if cleaned.is_empty() || cleaned == "-" || cleaned == "." {
        return Err(format!("tidak bisa parse angka dari {raw:?}"));
    }
    cleaned
        .parse::<f64>()
        .map_err(|e| format!("parse angka dari {raw:?}: {e}"))
}

fn scrape_header_js() -> &'static str {
    r#"(() => {
        const textOf = (sel) => {
            const el = document.querySelector(sel);
            if (!el) return null;
            const p = el.querySelector('p');
            if (!p) return null;
            const t = (p.textContent || '').trim();
            return t.length ? t : null;
        };
        return {
            trading_balance: textOf('[data-cy="porto-header-trading-balance-container"]'),
            invested: textOf('[data-cy="porto-header-invested-container"]'),
            open: textOf('[data-cy="porto-header-open-container"]'),
            net_profit_loss: textOf('[data-cy="porto-header-net-profit-loss-container"]'),
            total_equity: textOf('[data-cy="porto-header-total-equity-container"]'),
        };
    })()"#
}

fn header_ready_js() -> &'static str {
    r#"(() => {
        const sels = [
            '[data-cy="porto-header-trading-balance-container"]',
            '[data-cy="porto-header-invested-container"]',
            '[data-cy="porto-header-open-container"]',
            '[data-cy="porto-header-net-profit-loss-container"]',
            '[data-cy="porto-header-total-equity-container"]',
        ];
        for (const sel of sels) {
            const el = document.querySelector(sel);
            if (!el) return false;
            const p = el.querySelector('p');
            if (!p || !(p.textContent || '').trim()) return false;
        }
        return true;
    })()"#
}

async fn wait_porto_header(page: &Page, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = page
            .evaluate(header_ready_js())
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            println!("Portofolio equity: header trading balance / equity terlihat.");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                "Timeout menunggu porto-header (Trading Balance / Invested / Open / Net P/L / Total Equity)"
                    .into(),
            );
        }
        sleep(HEADER_POLL).await;
    }
}

async fn scrape_equity_dom(page: &Page) -> Result<EquityDomRaw, Box<dyn std::error::Error>> {
    let v = page.evaluate(scrape_header_js()).await?.into_value::<serde_json::Value>()?;
    let raw: EquityDomRaw = serde_json::from_value(v)
        .map_err(|e| format!("parse hasil DOM porto-header: {e}"))?;
    Ok(raw)
}

fn equity_rows_from_dom(raw: &EquityDomRaw) -> Result<Vec<(String, f64)>, Box<dyn std::error::Error>> {
    let pairs = [
        ("Trading Balance", raw.trading_balance.as_deref()),
        ("Invested", raw.invested.as_deref()),
        ("Open", raw.open.as_deref()),
        ("Net Profit Loss", raw.net_profit_loss.as_deref()),
        ("Total Equity", raw.total_equity.as_deref()),
    ];
    let mut out = Vec::with_capacity(pairs.len());
    for (nama, maybe) in pairs {
        let text = maybe
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("DOM porto-header kosong untuk {nama}"))?;
        let value = parse_equity_number(text)?;
        out.push((nama.to_string(), value));
    }
    Ok(out)
}

async fn upsert_portofolio_equity(
    session: &Session,
    keyspace: &str,
    rows: &[(String, f64)],
) -> Result<usize, Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio_equity (nama, value) VALUES (?, ?)"
        ))
        .await?;
    let mut n = 0usize;
    for (nama, value) in rows {
        session
            .execute_unpaged(&insert, (nama.as_str(), *value))
            .await?;
        n += 1;
        println!("INFO insert portofolio_equity [{n}/{}]: {nama} = {value}", rows.len());
    }
    Ok(n)
}

/// Buka halaman portfolio, tunggu header equity, DOM scrape → upsert `portofolio_equity`.
/// Harus dipanggil setelah PIN / mode trading siap, sebelum portfolio/order API.
pub async fn scrape_and_insert_portofolio_equity(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    println!("Portofolio equity: buka {STOCKBIT_PORTFOLIO_URL}...");
    goto_stockbit(page, STOCKBIT_PORTFOLIO_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    wait_porto_header(page, HEADER_WAIT_TIMEOUT).await?;

    let raw = scrape_equity_dom(page).await?;
    println!(
        "Portofolio equity DOM: TB={:?} Inv={:?} Open={:?} NPL={:?} TE={:?}",
        raw.trading_balance, raw.invested, raw.open, raw.net_profit_loss, raw.total_equity
    );

    let rows = equity_rows_from_dom(&raw)?;
    let n = upsert_portofolio_equity(session, keyspace, &rows).await?;
    println!("OK: {n} baris diupsert ke portofolio_equity.");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::parse_equity_number;

    #[test]
    fn parse_plain_with_commas() {
        assert_eq!(parse_equity_number("24,122,660").unwrap(), 24_122_660.0);
        assert_eq!(parse_equity_number("83,694,453").unwrap(), 83_694_453.0);
    }

    #[test]
    fn parse_net_profit_loss_strips_percent() {
        assert_eq!(
            parse_equity_number("-475,053 (-0.4%)").unwrap(),
            -475_053.0
        );
        assert_eq!(
            parse_equity_number("1,234 (0.5%)").unwrap(),
            1_234.0
        );
    }
}
