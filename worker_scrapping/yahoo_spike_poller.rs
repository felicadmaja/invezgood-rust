//! Poller Yahoo spike: cek pertama Senin–Jumat 09:00:00–09:00:59,
//! lalu setiap `INTERVAL_YAHOO_SPIKE_POLL_SECS` (.env, default 120) di jam kerja.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Local, NaiveDate, TimeZone, Timelike};
use rand::Rng;
use scylla::client::session::Session;
use tokio::sync::{watch, Mutex, Notify};

use crate::on_demand::{
    fetch_yahoo_price_spikes, is_scrape_hours_at, is_stockbit_poller_scrape_hours,
};
use crate::yahoo_atr::SpikeEmiten;

const DEFAULT_POLL_SECS: u64 = 120;

#[derive(Clone, Debug, PartialEq)]
pub struct YahooSpikeSnapshot {
    pub success: bool,
    pub message: String,
    pub data: Vec<SpikeEmiten>,
}

impl Default for YahooSpikeSnapshot {
    fn default() -> Self {
        Self {
            success: true,
            message: format!(
                "Menunggu poller Yahoo spike (cek pertama 09:00, lalu {}s)",
                poll_interval_secs()
            ),
            data: Vec::new(),
        }
    }
}

fn poll_interval_secs() -> u64 {
    std::env::var("INTERVAL_YAHOO_SPIKE_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_POLL_SECS)
        .max(1)
}

fn is_weekday(date: NaiveDate) -> bool {
    !matches!(
        date.weekday(),
        chrono::Weekday::Sat | chrono::Weekday::Sun
    )
}

fn local_at(date: NaiveDate, hour: u32, min: u32, sec: u32) -> DateTime<Local> {
    let naive = date
        .and_hms_opt(hour, min, sec)
        .expect("jam 09:00:ss valid");
    Local
        .from_local_datetime(&naive)
        .earliest()
        .expect("zona waktu lokal")
}

fn next_mon_fri_0900(now: DateTime<Local>, jitter_secs: u32) -> DateTime<Local> {
    let jitter = jitter_secs.min(59);
    let date = now.date_naive();
    if is_weekday(date) {
        let target = local_at(date, 9, 0, jitter);
        if now < target {
            return target;
        }
    }
    next_mon_fri_0900_after_date(date, jitter)
}

fn next_mon_fri_0900_after_date(after: NaiveDate, jitter_secs: u32) -> DateTime<Local> {
    let mut date = after.succ_opt().expect("tanggal");
    while !is_weekday(date) {
        date = date.succ_opt().expect("tanggal");
    }
    local_at(date, 9, 0, jitter_secs.min(59))
}

fn in_first_check_window(now: DateTime<Local>) -> bool {
    is_weekday(now.date_naive()) && now.hour() == 9 && now.minute() == 0
}

fn after_first_check_window(now: DateTime<Local>) -> bool {
    is_weekday(now.date_naive()) && (now.hour() > 9 || (now.hour() == 9 && now.minute() > 0))
}

fn sleep_secs_until(target: DateTime<Local>, now: DateTime<Local>) -> u64 {
    (target - now).num_seconds().max(0) as u64
}

/// Setelah sesi pagi/sore berakhir: siang hari ini (13:30/14:00) atau 09:00 hari kerja berikutnya.
fn next_resume_after_session(now: DateTime<Local>, jitter_secs: u32) -> DateTime<Local> {
    let today = now.date_naive();
    let mins = now.hour() * 60 + now.minute();
    match now.weekday() {
        chrono::Weekday::Fri => {
            if mins < 14 * 60 {
                local_at(today, 14, 0, 0)
            } else {
                next_mon_fri_0900_after_date(today, jitter_secs)
            }
        }
        chrono::Weekday::Sat | chrono::Weekday::Sun => next_mon_fri_0900(now, jitter_secs),
        _ => {
            if mins < 13 * 60 + 30 {
                local_at(today, 13, 30, 0)
            } else {
                next_mon_fri_0900_after_date(today, jitter_secs)
            }
        }
    }
}

/// Cek pertama hari kerja 09:00:00–09:00:59; seterusnya `INTERVAL_YAHOO_SPIKE_POLL_SECS` di jam kerja.
/// `false` = dibatalkan karena subscriber habis.
async fn wait_before_next_poll(
    poller: &YahooSpikePoller,
    last_first_check_date: &Option<NaiveDate>,
) -> bool {
    let now = Local::now();
    let today = now.date_naive();
    let first_done_today = *last_first_check_date == Some(today);

    if !first_done_today {
        if in_first_check_window(now) {
            println!(
                "Yahoo spike poller: cek pertama hari ini (09:00:{:02})",
                now.second()
            );
            return true;
        }
        if after_first_check_window(now) && is_stockbit_poller_scrape_hours() {
            println!(
                "Yahoo spike poller: cek pertama hari ini (terlewat 09:00, langsung cek)"
            );
            return true;
        }
        let jitter = rand::thread_rng().gen_range(0u32..=59);
        let target = next_mon_fri_0900(now, jitter);
        let wait_secs = sleep_secs_until(target, now).max(1);
        println!(
            "Yahoo spike poller: cek pertama Senin-Jumat 09:00:{:02} pada {} (tunggu {wait_secs}s)",
            jitter,
            target.format("%Y-%m-%d %H:%M:%S")
        );
        return poller.sleep_while_subscribed(wait_secs).await;
    }

    let wait_secs = poll_interval_secs();
    let jitter = rand::thread_rng().gen_range(0u32..=59);
    let next_first = next_mon_fri_0900_after_date(today, jitter);
    let wake = now + ChronoDuration::seconds(wait_secs as i64);
    if wake >= next_first {
        let wait_first = sleep_secs_until(next_first, now).max(1);
        println!(
            "Yahoo spike poller: cek pertama berikutnya {} (tunggu {wait_first}s, bukan interval {wait_secs}s)",
            next_first.format("%Y-%m-%d %H:%M:%S")
        );
        return poller.sleep_while_subscribed(wait_first).await;
    }
    if !is_scrape_hours_at(wake) {
        let resume = next_resume_after_session(now, jitter);
        let wait_window = sleep_secs_until(resume, now).max(1);
        println!(
            "Yahoo spike poller: di luar jam kerja — cek berikutnya {} (tunggu {wait_window}s)",
            resume.format("%Y-%m-%d %H:%M:%S")
        );
        return poller.sleep_while_subscribed(wait_window).await;
    }

    println!("Yahoo spike poller: cek berikutnya dalam {wait_secs}s");
    poller.sleep_while_subscribed(wait_secs).await
}

pub struct YahooSpikePoller {
    notify: watch::Sender<YahooSpikeSnapshot>,
    loop_started: Arc<Mutex<bool>>,
    subscriber_count: AtomicUsize,
    subs_notify: Notify,
}

impl YahooSpikePoller {
    pub fn new() -> Arc<Self> {
        let (notify, _) = watch::channel(YahooSpikeSnapshot::default());
        Arc::new(Self {
            notify,
            loop_started: Arc::new(Mutex::new(false)),
            subscriber_count: AtomicUsize::new(0),
            subs_notify: Notify::new(),
        })
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::SeqCst)
    }

    pub fn register_subscriber(&self) {
        let prev = self.subscriber_count.fetch_add(1, Ordering::SeqCst);
        if prev == 0 {
            println!("Yahoo spike poller: subscriber pertama — poller aktif");
        }
        self.subs_notify.notify_waiters();
    }

    pub fn unregister_subscriber(&self) {
        loop {
            let n = self.subscriber_count.load(Ordering::SeqCst);
            if n == 0 {
                break;
            }
            if self
                .subscriber_count
                .compare_exchange(n, n - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                if n == 1 {
                    println!("Yahoo spike poller: 0 subscriber — poller idle (tidak hit Yahoo)");
                }
                self.subs_notify.notify_waiters();
                break;
            }
        }
    }

    async fn wait_until_has_subscriber(&self) {
        loop {
            if self.subscriber_count() > 0 {
                return;
            }
            let notified = self.subs_notify.notified();
            tokio::pin!(notified);
            if self.subscriber_count() > 0 {
                return;
            }
            println!("Yahoo spike poller: idle (0 subscriber) — menunggu subscribe");
            notified.await;
        }
    }

    /// `true` bila durasi selesai dan masih ada subscriber.
    async fn sleep_while_subscribed(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if self.subscriber_count() == 0 {
                return false;
            }
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return self.subscriber_count() > 0;
            }
            let notified = self.subs_notify.notified();
            tokio::select! {
                _ = tokio::time::sleep(rem) => return self.subscriber_count() > 0,
                _ = notified => {}
            }
        }
    }

    pub fn latest(&self) -> YahooSpikeSnapshot {
        self.notify.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<YahooSpikeSnapshot> {
        self.notify.subscribe()
    }

    pub async fn ensure_loop_running(self: &Arc<Self>, session: Arc<Session>) {
        let mut started = self.loop_started.lock().await;
        if *started {
            return;
        }
        let cached = crate::yahoo_spike_cache::today_details().await;
        if !cached.is_empty() {
            let n = cached.len();
            let _ = self.notify.send(YahooSpikeSnapshot {
                success: true,
                message: format!("{n} emiten spike hari ini"),
                data: cached,
            });
        }
        let poller = Arc::clone(self);
        tokio::spawn(async move {
            poller.run_loop(session).await;
        });
        *started = true;
        println!(
            "Yahoo spike poller: siap (jalan hanya jika ≥1 subscriber; cek pertama 09:00, lalu {}s)",
            poll_interval_secs()
        );
    }

    async fn publish(&self, snap: YahooSpikeSnapshot) {
        let _ = self.notify.send(snap);
    }

    async fn run_loop(self: Arc<Self>, session: Arc<Session>) {
        let mut last_first_check_date: Option<NaiveDate> = None;
        loop {
            self.wait_until_has_subscriber().await;
            if !wait_before_next_poll(&self, &last_first_check_date).await {
                continue;
            }
            if self.subscriber_count() == 0 {
                continue;
            }
            if !is_stockbit_poller_scrape_hours() {
                continue;
            }
            if crate::yahoo_market_holiday::is_spike_poller_holiday().await {
                let today = Local::now().date_naive();
                println!(
                    "Yahoo spike poller: hari libur (Sabtu/Minggu atau invezgood.hari_libur) — poller tidak dijalankan"
                );
                let acc = crate::yahoo_spike_cache::today_details().await;
                self.publish(YahooSpikeSnapshot {
                    success: true,
                    message: format!("hari libur (invezgood.hari_libur, {today})"),
                    data: acc,
                })
                .await;
                last_first_check_date = Some(today);
                let jitter = rand::thread_rng().gen_range(0u32..=59);
                let next = next_mon_fri_0900_after_date(today, jitter);
                let wait = sleep_secs_until(next, Local::now()).max(1);
                println!(
                    "Yahoo spike poller: cek berikutnya {} (tunggu {wait}s)",
                    next.format("%Y-%m-%d %H:%M:%S")
                );
                let _ = self.sleep_while_subscribed(wait).await;
                continue;
            }

            println!(
                "Yahoo spike poller: tick ({} subscriber)",
                self.subscriber_count()
            );
            match fetch_yahoo_price_spikes(session.as_ref()).await {
                Ok(acc) => {
                    let message = format!("{} emiten spike hari ini", acc.len());
                    self.publish(YahooSpikeSnapshot {
                        success: true,
                        message,
                        data: acc,
                    })
                    .await;
                }
                Err(e) => {
                    eprintln!("Yahoo spike poller: {e}");
                    let acc = crate::yahoo_spike_cache::today_details().await;
                    self.publish(YahooSpikeSnapshot {
                        success: false,
                        message: e,
                        data: acc,
                    })
                    .await;
                }
            }
            last_first_check_date = Some(Local::now().date_naive());
        }
    }
}
