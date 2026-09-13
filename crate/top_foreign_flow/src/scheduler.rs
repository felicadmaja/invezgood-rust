//! Scheduler `GetTopForeignFlowByTanggal`: setiap hari jam 04:00 waktu lokal server
//! (override env `TOP_FOREIGN_FLOW_SYNC_HOUR`, `TOP_FOREIGN_FLOW_SYNC_MINUTE`).
//! Sync tanggal kemarin; lewati bila kemarin Sabtu/Minggu atau hari libur nasional.

use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, TimeZone, Timelike};
use scylla::client::session::Session;

const DEFAULT_SYNC_HOUR: u32 = 4;
const DEFAULT_SYNC_MINUTE: u32 = 0;

fn sync_hour_from_env() -> u32 {
    std::env::var("TOP_FOREIGN_FLOW_SYNC_HOUR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SYNC_HOUR)
        .min(23)
}

fn sync_minute_from_env() -> u32 {
    std::env::var("TOP_FOREIGN_FLOW_SYNC_MINUTE")
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

fn next_sync_at(now: DateTime<Local>, hour: u32, min: u32) -> DateTime<Local> {
    let date = now.date_naive();
    let target = local_at(date, hour, min, 0);
    if now < target {
        target
    } else {
        local_at(date.succ_opt().expect("tanggal"), hour, min, 0)
    }
}

fn missed_today_sync(
    now: DateTime<Local>,
    hour: u32,
    min: u32,
    last_run_date: Option<NaiveDate>,
) -> bool {
    let today = now.date_naive();
    if last_run_date == Some(today) {
        return false;
    }
    now.hour() > hour || (now.hour() == hour && now.minute() >= min)
}

fn is_weekend(date: NaiveDate) -> bool {
    matches!(
        date.weekday(),
        chrono::Weekday::Sat | chrono::Weekday::Sun
    )
}

async fn should_skip_yesterday(yesterday: NaiveDate) -> Option<&'static str> {
    if is_weekend(yesterday) {
        return Some("Sabtu/Minggu");
    }
    if market_holiday::is_market_holiday_on(yesterday).await {
        return Some("hari libur");
    }
    None
}

async fn run_sync_yesterday(session: Arc<Session>) {
    let yesterday = Local::now().date_naive() - Duration::days(1);

    if let Some(reason) = should_skip_yesterday(yesterday).await {
        eprintln!(
            "GetTopForeignFlowByTanggal scheduler: lewati {} ({reason})",
            yesterday.format("%Y-%m-%d")
        );
        return;
    }

    match crate::sync::sync_trade_date(session, yesterday).await {
        Ok(outcome) => {
            let source = if outcome.cached {
                "cache Scylla"
            } else {
                "fetch Invezgo"
            };
            eprintln!(
                "GetTopForeignFlowByTanggal scheduler: {} {source}, {} baris (upsert {})",
                yesterday.format("%Y-%m-%d"),
                outcome.rows.len(),
                outcome.saved
            );
        }
        Err(e) => eprintln!(
            "GetTopForeignFlowByTanggal scheduler gagal {}: {e}",
            yesterday.format("%Y-%m-%d")
        ),
    }
}

/// Loop background: sync top foreign flow tanggal kemarin setiap hari jam 04:00 lokal.
pub fn spawn_daily_top_foreign_flow_sync(session: Arc<Session>) {
    tokio::spawn(async move {
        let hour = sync_hour_from_env();
        let min = sync_minute_from_env();
        let mut last_run_date: Option<NaiveDate> = None;

        loop {
            let now = Local::now();

            if missed_today_sync(now, hour, min, last_run_date) {
                eprintln!(
                    "GetTopForeignFlowByTanggal scheduler: catch-up (terlewat {hour:02}:{min:02} hari ini)"
                );
                run_sync_yesterday(session.clone()).await;
            }

            let now = Local::now();
            let target = next_sync_at(now, hour, min);
            let wait_secs = (target - now).num_seconds().max(1) as u64;
            eprintln!(
                "GetTopForeignFlowByTanggal scheduler: sync berikutnya {} (tunggu {wait_secs}s)",
                target.format("%Y-%m-%d %H:%M:%S")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

            run_sync_yesterday(session.clone()).await;
            last_run_date = Some(Local::now().date_naive());
        }
    });
}
