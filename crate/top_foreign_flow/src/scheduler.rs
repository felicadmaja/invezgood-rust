//! Scheduler `GetTopForeignFlowByTanggal`: setiap hari 08:30 waktu lokal (override env),
//! sync tanggal kemarin bila bukan Sabtu/Minggu/hari libur nasional.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Local, NaiveDate, TimeZone, Timelike};
use scylla::client::session::Session;

const DEFAULT_SYNC_HOUR: u32 = 8;
const DEFAULT_SYNC_MINUTE: u32 = 30;

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
        local_at(
            date.succ_opt().expect("tanggal"),
            hour,
            min,
            0,
        )
    }
}

fn missed_today_sync(
    now: DateTime<Local>,
    hour: u32,
    min: u32,
    last_sync_date: Option<NaiveDate>,
) -> bool {
    let today = now.date_naive();
    if last_sync_date == Some(today) {
        return false;
    }
    let now_mins = now.hour() * 60 + now.minute();
    let target_mins = hour * 60 + min;
    now_mins > target_mins
}

async fn run_sync(session: Arc<Session>) {
    let yesterday = Local::now().date_naive() - ChronoDuration::days(1);

    if crate::sync::is_non_trading_day(yesterday).await {
        eprintln!(
            "GetTopForeignFlowByTanggal scheduler: skip {yesterday} (Sabtu/Minggu atau invezgood.hari_libur)"
        );
        return;
    }

    match crate::sync::sync_trade_date(session, yesterday).await {
        Ok(outcome) => {
            if outcome.cached {
                eprintln!(
                    "GetTopForeignFlowByTanggal scheduler {yesterday}: cache Scylla, {} baris",
                    outcome.rows.len()
                );
            } else {
                eprintln!(
                    "GetTopForeignFlowByTanggal scheduler {yesterday}: {} baris upsert Invezgo, {} baris Scylla",
                    outcome.saved,
                    outcome.rows.len()
                );
            }
        }
        Err(e) => eprintln!("GetTopForeignFlowByTanggal scheduler {yesterday} gagal: {e}"),
    }
}

/// Loop background: sync top foreign flow tanggal kemarin setiap hari 08:30 lokal.
pub fn spawn_daily_top_foreign_flow_sync(session: Arc<Session>) {
    tokio::spawn(async move {
        let hour = sync_hour_from_env();
        let min = sync_minute_from_env();
        let mut last_sync_date: Option<NaiveDate> = None;

        loop {
            let now = Local::now();

            if missed_today_sync(now, hour, min, last_sync_date) {
                eprintln!(
                    "GetTopForeignFlowByTanggal scheduler: catch-up (terlewat {hour:02}:{min:02} hari ini)"
                );
                run_sync(session.clone()).await;
            }

            let now = Local::now();
            let target = next_sync_at(now, hour, min);
            let wait_secs = (target - now).num_seconds().max(1) as u64;
            eprintln!(
                "GetTopForeignFlowByTanggal scheduler: sync berikutnya {} (tunggu {wait_secs}s)",
                target.format("%Y-%m-%d %H:%M:%S")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

            run_sync(session.clone()).await;
            last_sync_date = Some(Local::now().date_naive());
        }
    });
}
