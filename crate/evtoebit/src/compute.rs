use std::sync::Arc;
use std::time::Duration;

use scylla::client::session::Session;

use crate::aggregate::{aggregate_median, max_multiple};
use crate::pb::GetMedianEvToEbitdaResponse;
use crate::universe::load_universe;
use crate::yahoo::{YahooClient, throttle};

fn max_codes() -> Option<usize> {
    std::env::var("EVTOEBIT_MAX_CODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
}

pub async fn compute_median(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
) -> Result<GetMedianEvToEbitdaResponse, String> {
    let mut universe = load_universe(session.as_ref()).await?;
    if universe.is_empty() {
        return Err("universe kosong dari invezgood.stock_list".into());
    }
    if let Some(limit) = max_codes() {
        universe.truncate(limit);
    }

    eprintln!(
        "GetMedianEVToEbitda compute {} emiten via Yahoo Finance (Rust)",
        universe.len()
    );

    let mut metrics = Vec::with_capacity(universe.len());
    for (i, row) in universe.iter().enumerate() {
        let result = yahoo.fetch_emiten(&row.kode).await;
        metrics.push((row.kode.clone(), result));
        if (i + 1) % 25 == 0 {
            eprintln!("  {}/{}", i + 1, universe.len());
        }
        if i + 1 < universe.len() {
            throttle().await;
        }
    }

    let rows = aggregate_median(&universe, &metrics, max_multiple());
    Ok(GetMedianEvToEbitdaResponse {
        success: true,
        message: format!(
            "median EV/EBIT dari {} emiten BEI (Yahoo Finance), {} baris sektor/sub-sektor",
            universe.len(),
            rows.len()
        ),
        rows,
    })
}

pub fn cache_ttl() -> Duration {
    let secs = std::env::var("EVTOEBIT_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24 * 60 * 60);
    Duration::from_secs(secs)
}
