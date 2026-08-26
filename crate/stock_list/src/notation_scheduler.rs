//! Scheduler sync notation Invezgo: setiap hari jam 08:00 waktu lokal (override: `NOTATION_SYNC_HOUR`).

use std::sync::Arc;

use chrono::{DateTime, Local, NaiveDate, TimeZone, Timelike};
use scylla::client::session::Session;

const DEFAULT_SYNC_HOUR: u32 = 8;

fn sync_hour_from_env() -> u32 {
    std::env::var("NOTATION_SYNC_HOUR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SYNC_HOUR)
        .min(23)
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

fn next_sync_at(now: DateTime<Local>, hour: u32) -> DateTime<Local> {
    let date = now.date_naive();
    let target = local_at(date, hour, 0, 0);
    if now < target {
        target
    } else {
        local_at(date.succ_opt().expect("tanggal"), hour, 0, 0)
    }
}

fn missed_today_sync(now: DateTime<Local>, hour: u32, last_sync_date: Option<NaiveDate>) -> bool {
    let today = now.date_naive();
    if last_sync_date == Some(today) {
        return false;
    }
    now.hour() > hour || (now.hour() == hour && now.minute() > 0)
}

async fn run_sync(session: Arc<Session>) {
    match crate::invezgo::fetch_and_save_notation(session).await {
        Ok((updated, skipped)) => {
            eprintln!(
                "UpdateNotationInvezgo scheduler {updated} code diupdate, {skipped} dilewati"
            );
        }
        Err(e) => eprintln!("UpdateNotationInvezgo scheduler gagal: {e}"),
    }
}

/// Loop background: sync notation Invezgo setiap hari jam 08:00 lokal. Dipanggil dari `main`.
pub fn spawn_daily_notation_sync(session: Arc<Session>) {
    tokio::spawn(async move {
        let hour = sync_hour_from_env();
        let mut last_sync_date: Option<NaiveDate> = None;

        loop {
            let now = Local::now();

            if missed_today_sync(now, hour, last_sync_date) {
                eprintln!(
                    "UpdateNotationInvezgo scheduler: catch-up (terlewat {hour:02}:00 hari ini)"
                );
                run_sync(session.clone()).await;
            }

            let now = Local::now();
            let target = next_sync_at(now, hour);
            let wait_secs = (target - now).num_seconds().max(1) as u64;
            eprintln!(
                "UpdateNotationInvezgo scheduler: sync berikutnya {} (tunggu {wait_secs}s)",
                target.format("%Y-%m-%d %H:%M:%S")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

            run_sync(session.clone()).await;
            last_sync_date = Some(Local::now().date_naive());
        }
    });
}
