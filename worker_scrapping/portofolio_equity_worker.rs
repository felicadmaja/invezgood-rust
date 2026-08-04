//! Parse header equity dari `GET carina.stockbit.com/portfolio/v2/list`
//! (`data.summary`) → upsert `portofolio_equity`.
//!
//! Mapping (nama baris → path JSON):
//! - Trading Balance → `summary.trading.balance`
//! - Invested → `summary.amount.invested`
//! - Open → `summary.amount.allocated`
//! - Net Profit Loss → `summary.profit_loss.net`
//! - Total Equity → `summary.equity`

use scylla::client::session::Session;
use serde_json::Value;

fn json_f64(v: &Value, path: &[&str]) -> Option<f64> {
    let mut cur = v;
    for p in path {
        cur = cur.get(*p)?;
    }
    cur.as_f64()
        .or_else(|| cur.as_i64().map(|n| n as f64))
        .or_else(|| cur.as_u64().map(|n| n as f64))
}

/// Ambil 5 baris equity dari body JSON `portfolio/v2/list`.
pub fn equity_rows_from_portfolio_json(
    v: &Value,
) -> Result<Vec<(String, f64)>, Box<dyn std::error::Error>> {
    let summary = v
        .pointer("/data/summary")
        .ok_or("portfolio/v2/list: data.summary tidak ada")?;

    let pairs: [(&str, Option<f64>); 5] = [
        (
            "Trading Balance",
            json_f64(summary, &["trading", "balance"]),
        ),
        ("Invested", json_f64(summary, &["amount", "invested"])),
        ("Open", json_f64(summary, &["amount", "allocated"])),
        (
            "Net Profit Loss",
            json_f64(summary, &["profit_loss", "net"]),
        ),
        ("Total Equity", json_f64(summary, &["equity"])),
    ];

    let mut out = Vec::with_capacity(pairs.len());
    for (nama, maybe) in pairs {
        let value = maybe.ok_or_else(|| {
            format!("portfolio/v2/list: summary field untuk `{nama}` tidak ada / bukan angka")
        })?;
        out.push((nama.to_string(), value));
    }
    Ok(out)
}

pub async fn upsert_portofolio_equity(
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
    }
    Ok(n)
}

/// Upsert `portofolio_equity` dari satu body JSON `portfolio/v2/list`.
pub async fn upsert_portofolio_equity_from_json(
    session: &Session,
    keyspace: &str,
    v: &Value,
) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = equity_rows_from_portfolio_json(v)?;
    println!(
        "\x1b[32mPortofolio equity API summary: {}\x1b[0m",
        rows
            .iter()
            .map(|(n, v)| format!("{n}={v}"))
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let n = upsert_portofolio_equity(session, keyspace, &rows).await?;
    println!("OK: {n} baris diupsert ke portofolio_equity.");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::equity_rows_from_portfolio_json;
    use serde_json::json;

    #[test]
    fn parse_summary_maps_five_rows() {
        let v = json!({
            "data": {
                "summary": {
                    "trading": { "balance": 5343.28 },
                    "amount": {
                        "invested": 161387412.22,
                        "allocated": 0.0,
                        "credit_limit": 5343.28
                    },
                    "profit_loss": {
                        "net": -6584012.22,
                        "unrealised": -6584012.22,
                        "realised": 0
                    },
                    "gain": -0.04079497,
                    "equity": 154808743.28,
                    "debt": { "ratio": 0, "total": 0 }
                }
            }
        });
        let rows = equity_rows_from_portfolio_json(&v).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].0, "Trading Balance");
        assert!((rows[0].1 - 5343.28).abs() < 1e-9);
        assert_eq!(rows[1].0, "Invested");
        assert!((rows[1].1 - 161387412.22).abs() < 1e-6);
        assert_eq!(rows[2].0, "Open");
        assert!((rows[2].1 - 0.0).abs() < 1e-9);
        assert_eq!(rows[3].0, "Net Profit Loss");
        assert!((rows[3].1 - (-6584012.22)).abs() < 1e-6);
        assert_eq!(rows[4].0, "Total Equity");
        assert!((rows[4].1 - 154808743.28).abs() < 1e-6);
    }
}
