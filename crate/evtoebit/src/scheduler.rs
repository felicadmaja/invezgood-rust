//! Scheduler `GetMedianEVToEbitdaFromYahooFinance`: setiap hari jam 00:00 waktu lokal
//! (override env `EVTOEBIT_SYNC_HOUR`, `EVTOEBIT_SYNC_MINUTE`).

use std::sync::Arc;

use chrono::{DateTime, Local, NaiveDate, TimeZone};
use scylla::client::session::Session;

use crate::cache::MedianCache;
use crate::sync::sync_median_from_yahoo_to_scylla;
use crate::yahoo::YahooClient;

const DEFAULT_SYNC_HOUR: u32 = 0;
const DEFAULT_SYNC_MINUTE: u32 = 0;

fn sync_hour_from_env() -> u32 {
    std::env::var("EVTOEBIT_SYNC_HOUR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SYNC_HOUR)
        .min(23)
}

fn sync_minute_from_env() -> u32 {
    std::env::var("EVTOEBIT_SYNC_MINUTE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SYNC_MINUTE)
        .min(59)
}

fn local_at(date: NaiveDate, hour: u32, min: u32, sec: u32) -> DateTime<Local> {
    let naive = date
        .and_hms_opt(hour, min, sec)
        .expect("waktu scheduler valid");
    Local
        .from_local_datetime(&naive)
        .earliest()
        .expect("zona waktu lokal")
}

fn next_daily_sync_at(now: DateTime<Local>, hour: u32, min: u32) -> DateTime<Local> {
    let today = now.date_naive();
    let target = local_at(today, hour, min, 0);
    if now < target {
        return target;
    }
    local_at(today.succ_opt().expect("tanggal scheduler valid"), hour, min, 0)
}

async fn run_sync(session: Arc<Session>, yahoo: Arc<YahooClient>, cache: Arc<MedianCache>) {
    match sync_median_from_yahoo_to_scylla(session, yahoo, Some(cache)).await {
        Ok((n, message)) => {
            eprintln!("GetMedianEVToEbitdaFromYahooFinance scheduler: {message}, upsert {n} baris")
        }
        Err(e) => eprintln!("GetMedianEVToEbitdaFromYahooFinance scheduler gagal: {e}"),
    }
}

/// Loop background: sync Yahoo → Scylla setiap hari jam 00:00 lokal.
/// Tidak ada catch-up saat restart — hanya jadwal harian atau invoke RPC user.
pub fn spawn_daily_evtoebit_sync(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
    cache: Arc<MedianCache>,
) {
    tokio::spawn(async move {
        let hour = sync_hour_from_env();
        let min = sync_minute_from_env();

        loop {
            let now = Local::now();
            let target = next_daily_sync_at(now, hour, min);
            let wait_secs = (target - now).num_seconds().max(1) as u64;
            eprintln!(
                "GetMedianEVToEbitdaFromYahooFinance scheduler: sync berikutnya {} (tunggu {wait_secs}s)",
                target.format("%Y-%m-%d %H:%M:%S")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

            run_sync(session.clone(), yahoo.clone(), cache.clone()).await;
        }
    });
}
