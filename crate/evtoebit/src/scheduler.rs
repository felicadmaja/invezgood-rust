//! Scheduler `GetMedianEVToEbitdaFromYahooFinance`: tanggal 1 setiap bulan jam 00:00 waktu lokal
//! (override env `EVTOEBIT_SYNC_DAY`, `EVTOEBIT_SYNC_HOUR`, `EVTOEBIT_SYNC_MINUTE`).

use std::sync::Arc;

use chrono::{DateTime, Datelike, Local, NaiveDate, TimeZone, Timelike};
use scylla::client::session::Session;

use crate::cache::MedianCache;
use crate::sync::sync_median_from_yahoo_to_scylla;
use crate::yahoo::YahooClient;

const DEFAULT_SYNC_DAY: u32 = 1;
const DEFAULT_SYNC_HOUR: u32 = 0;
const DEFAULT_SYNC_MINUTE: u32 = 0;

fn sync_day_from_env() -> u32 {
    std::env::var("EVTOEBIT_SYNC_DAY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SYNC_DAY)
        .clamp(1, 28)
}

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

fn month_slot_at(year: i32, month: u32, day: u32, hour: u32, min: u32) -> DateTime<Local> {
    let date = NaiveDate::from_ymd_opt(year, month, day).expect("tanggal scheduler valid");
    local_at(date, hour, min, 0)
}

fn next_monthly_sync_at(now: DateTime<Local>, day: u32, hour: u32, min: u32) -> DateTime<Local> {
    let y = now.year();
    let m = now.month();
    let target = month_slot_at(y, m, day, hour, min);
    if now < target {
        return target;
    }
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    month_slot_at(ny, nm, day, hour, min)
}

fn missed_monthly_sync(
    now: DateTime<Local>,
    day: u32,
    hour: u32,
    min: u32,
    last_sync_month: Option<(i32, u32)>,
) -> bool {
    let y = now.year();
    let m = now.month();
    if last_sync_month == Some((y, m)) {
        return false;
    }
    if now.day() > day {
        return true;
    }
    if now.day() == day {
        let now_mins = now.hour() * 60 + now.minute();
        let target_mins = hour * 60 + min;
        return now_mins >= target_mins;
    }
    false
}

async fn run_sync(session: Arc<Session>, yahoo: Arc<YahooClient>, cache: Arc<MedianCache>) {
    match sync_median_from_yahoo_to_scylla(session, yahoo, Some(cache)).await {
        Ok((n, message)) => {
            eprintln!("GetMedianEVToEbitdaFromYahooFinance scheduler: {message}, upsert {n} baris")
        }
        Err(e) => eprintln!("GetMedianEVToEbitdaFromYahooFinance scheduler gagal: {e}"),
    }
}

/// Loop background: sync Yahoo → Scylla setiap tanggal 1 jam 00:00 lokal.
pub fn spawn_monthly_evtoebit_sync(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
    cache: Arc<MedianCache>,
) {
    tokio::spawn(async move {
        let day = sync_day_from_env();
        let hour = sync_hour_from_env();
        let min = sync_minute_from_env();
        let mut last_sync_month: Option<(i32, u32)> = None;

        loop {
            let now = Local::now();

            if missed_monthly_sync(now, day, hour, min, last_sync_month) {
                eprintln!(
                    "GetMedianEVToEbitdaFromYahooFinance scheduler: catch-up (terlewat {day} \
                     {hour:02}:{min:02} bulan ini)"
                );
                run_sync(session.clone(), yahoo.clone(), cache.clone()).await;
            }

            let now = Local::now();
            let target = next_monthly_sync_at(now, day, hour, min);
            let wait_secs = (target - now).num_seconds().max(1) as u64;
            eprintln!(
                "GetMedianEVToEbitdaFromYahooFinance scheduler: sync berikutnya {} (tunggu {wait_secs}s)",
                target.format("%Y-%m-%d %H:%M:%S")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

            run_sync(session.clone(), yahoo.clone(), cache.clone()).await;
            let now = Local::now();
            last_sync_month = Some((now.year(), now.month()));
        }
    });
}
