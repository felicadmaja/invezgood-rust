use std::sync::Arc;

use grpc_stream::send_or_break;
use scylla::client::session::Session;
use tokio::sync::mpsc;
use tonic::Status;

use crate::aggregate::{aggregate_median, max_multiple};
use crate::pb::GetMedianEvToEbitdaFromYahooFinanceResponse;
use crate::universe::load_universe;
use crate::yahoo::{YahooClient, throttle};

fn max_codes() -> Option<usize> {
    std::env::var("EVTOEBIT_MAX_CODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
}

fn progress_response(done: i32, total: i32) -> GetMedianEvToEbitdaFromYahooFinanceResponse {
    GetMedianEvToEbitdaFromYahooFinanceResponse {
        success: true,
        message: format!("fetch Yahoo {done}/{total} emiten"),
        rows: vec![],
        fetch_done: Some(done),
        fetch_total: Some(total),
    }
}

type ProgressTx = mpsc::Sender<Result<GetMedianEvToEbitdaFromYahooFinanceResponse, Status>>;

async fn push_progress(progress_tx: &ProgressTx, done: i32, total: i32) -> Result<(), String> {
    if send_or_break(progress_tx, Ok(progress_response(done, total))).await {
        Ok(())
    } else {
        Err("client disconnect".into())
    }
}

pub async fn compute_median(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
    progress_tx: Option<ProgressTx>,
) -> Result<GetMedianEvToEbitdaFromYahooFinanceResponse, String> {
    let mut universe = load_universe(session.as_ref()).await?;
    if universe.is_empty() {
        return Err("universe kosong dari invezgood.stock_list".into());
    }
    if let Some(limit) = max_codes() {
        universe.truncate(limit);
    }

    let total = universe.len() as i32;
    eprintln!(
        "GetMedianEVToEbitdaFromYahooFinance compute {} emiten via Yahoo Finance (Rust)",
        universe.len()
    );

    if let Some(ref tx) = progress_tx {
        push_progress(tx, 0, total).await?;
    }

    let mut metrics = Vec::with_capacity(universe.len());
    for (i, row) in universe.iter().enumerate() {
        let result = yahoo.fetch_emiten(&row.kode).await;
        metrics.push((row.kode.clone(), result));
        let done = (i + 1) as i32;
        if (i + 1) % 25 == 0 {
            eprintln!("  {}/{}", i + 1, universe.len());
        }
        if let Some(ref tx) = progress_tx {
            push_progress(tx, done, total).await?;
        }
        if i + 1 < universe.len() {
            throttle().await;
        }
    }

    let rows = aggregate_median(&universe, &metrics, max_multiple());
    Ok(GetMedianEvToEbitdaFromYahooFinanceResponse {
        success: true,
        message: format!(
            "median EV/EBIT dari {} emiten BEI (Yahoo Finance), {} sektor",
            universe.len(),
            rows.len()
        ),
        rows,
        fetch_done: None,
        fetch_total: None,
    })
}
