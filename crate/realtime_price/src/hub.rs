//! Poller per `emiten_name`: GET Stockbit 1×/30 detik hanya jika ada subscriber
//! dan dalam jam operasional (bukan libur); di luar jam/libur → Redis
//! (seed GET bila Redis kosong, kecuali hari sudah ditandai libur).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::fetch::fetch_realtime_price;
use crate::hours::{can_detect_holiday_by_volume, is_realtime_price_hours};
use crate::redis_cache;
use crate::GetRealtimePriceFromStockbitResponse;

const POLL_INTERVAL: Duration = Duration::from_secs(30);
const BROADCAST_CAP: usize = 16;

struct Feed {
    code: String,
    tx: broadcast::Sender<GetRealtimePriceFromStockbitResponse>,
    subscribers: AtomicUsize,
    poller: Mutex<Option<JoinHandle<()>>>,
    last: Mutex<Option<GetRealtimePriceFromStockbitResponse>>,
}

impl Feed {
    fn new(code: String) -> Arc<Self> {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Arc::new(Self {
            code,
            tx,
            subscribers: AtomicUsize::new(0),
            poller: Mutex::new(None),
            last: Mutex::new(None),
        })
    }

    async fn ensure_poller(self: &Arc<Self>) {
        let mut slot = self.poller.lock().await;
        if let Some(h) = slot.as_ref() {
            if !h.is_finished() {
                return;
            }
        }
        let feed = Arc::clone(self);
        let handle = tokio::spawn(async move {
            feed.run_poller().await;
        });
        *slot = Some(handle);
    }

    async fn publish(self: &Arc<Self>, resp: GetRealtimePriceFromStockbitResponse) {
        {
            let mut last = self.last.lock().await;
            *last = Some(resp.clone());
        }
        let _ = self.tx.send(resp);
    }

    /// Kirim cache Redis / memory (tanpa GET).
    async fn publish_cache_only(self: &Arc<Self>, reason: &str) {
        if let Some(cached) = redis_cache::get(&self.code).await {
            println!(
                "RealtimePrice emiten_name={}: {reason} → Redis cache (date={} time={} price={} volume={})",
                self.code, cached.date, cached.time, cached.price, cached.volume
            );
            self.publish(cached).await;
        } else if let Some(mem) = self.last.lock().await.clone() {
            println!(
                "RealtimePrice emiten_name={}: {reason} → memory last (Redis miss)",
                self.code
            );
            let _ = self.tx.send(mem);
        } else {
            println!(
                "RealtimePrice emiten_name={}: {reason} — belum ada cache Redis/memory",
                self.code
            );
        }
    }

    async fn scrape_and_store(self: &Arc<Self>) {
        match stockbit_browser::ensure_stockbit_bearer().await {
            Ok(bearer) => match fetch_realtime_price(&self.code, &bearer).await {
                Ok(resp) => {
                    println!(
                        "RealtimePrice scrape emiten_name={} OK: symbol={} price={} formatted_price={} date={} time={} volume={}",
                        self.code,
                        resp.symbol,
                        resp.price,
                        resp.formatted_price,
                        resp.date,
                        resp.time,
                        resp.volume
                    );

                    // Deteksi tanggal merah / cuti bersama: weekday, ≥09:10, volume masih 0.
                    if can_detect_holiday_by_volume() && resp.volume == 0 {
                        if !redis_cache::is_holiday_today().await {
                            redis_cache::declare_holiday_today().await;
                        }
                        // Jangan timpa Redis harga terakhir dengan volume=0; kirim cache.
                        self.publish_cache_only("hari libur (volume=0 setelah 09:10)")
                            .await;
                        // Bila sama sekali belum ada cache, simpan seed volume=0 agar stream ada isi.
                        if redis_cache::get(&self.code).await.is_none()
                            && self.last.lock().await.is_none()
                        {
                            redis_cache::set(&self.code, &resp).await;
                            self.publish(resp).await;
                        }
                        return;
                    }

                    redis_cache::set(&self.code, &resp).await;
                    self.publish(resp).await;
                }
                Err(e) => {
                    eprintln!(
                        "RealtimePrice scrape emiten_name={} gagal: {e}",
                        self.code
                    );
                }
            },
            Err(e) => {
                eprintln!(
                    "RealtimePrice scrape emiten_name={} bearer gagal: {e}",
                    self.code
                );
            }
        }
    }

    async fn run_poller(self: Arc<Self>) {
        println!(
            "RealtimePrice poller {}: start (subscriber aktif)",
            self.code
        );
        while self.subscribers.load(Ordering::SeqCst) > 0 {
            let holiday = redis_cache::is_holiday_today().await;
            let in_hours = is_realtime_price_hours();
            let redis_hit = redis_cache::get(&self.code).await;

            if holiday {
                // Hari sudah ditandai libur → hentikan GET API; hanya Redis/cache.
                self.publish_cache_only("hari libur (tanggal merah/cuti bersama)")
                    .await;
            } else if !in_hours {
                if let Some(cached) = redis_hit {
                    println!(
                        "RealtimePrice emiten_name={}: diluar jam operasional → Redis cache (date={} time={} price={})",
                        self.code, cached.date, cached.time, cached.price
                    );
                    self.publish(cached).await;
                } else {
                    println!(
                        "RealtimePrice scrape emiten_name={}: diluar jam & Redis kosong → GET seed...",
                        self.code
                    );
                    self.scrape_and_store().await;
                }
            } else {
                println!(
                    "RealtimePrice scrape emiten_name={}: GET emitten/{}/info (jam operasional)...",
                    self.code, self.code
                );
                self.scrape_and_store().await;
            }

            if !self.sleep_until_next_tick().await {
                return;
            }
        }
        println!(
            "RealtimePrice poller {}: stop (tidak ada subscriber)",
            self.code
        );
    }

    /// `false` = tidak ada subscriber lagi, hentikan poller.
    async fn sleep_until_next_tick(&self) -> bool {
        for _ in 0..POLL_INTERVAL.as_secs() {
            if self.subscribers.load(Ordering::SeqCst) == 0 {
                println!(
                    "RealtimePrice poller {}: stop (tidak ada subscriber) — tidak GET API lagi",
                    self.code
                );
                return false;
            }
            sleep(Duration::from_secs(1)).await;
        }
        true
    }
}

/// Guard: +1 subscriber saat dibuat, −1 saat drop; start/stop poller otomatis.
pub struct Subscription {
    feed: Arc<Feed>,
    rx: broadcast::Receiver<GetRealtimePriceFromStockbitResponse>,
    last: Option<GetRealtimePriceFromStockbitResponse>,
}

impl Subscription {
    pub async fn next(&mut self) -> Option<GetRealtimePriceFromStockbitResponse> {
        if let Some(first) = self.last.take() {
            return Some(first);
        }
        loop {
            match self.rx.recv().await {
                Ok(msg) => return Some(msg),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let prev = self.feed.subscribers.fetch_sub(1, Ordering::SeqCst);
        println!(
            "RealtimePrice {}: unsubscribe (sisa {})",
            self.feed.code,
            prev.saturating_sub(1)
        );
    }
}

#[derive(Default)]
pub struct RealtimePriceHub {
    feeds: Mutex<HashMap<String, Arc<Feed>>>,
}

impl RealtimePriceHub {
    pub fn new() -> Self {
        Self {
            feeds: Mutex::new(HashMap::new()),
        }
    }

    /// Subscribe harga untuk `code` (UPPERCASE). Memulai poller bila subscriber pertama.
    pub async fn subscribe(&self, code: String) -> Subscription {
        let feed = {
            let mut map = self.feeds.lock().await;
            map.entry(code.clone())
                .or_insert_with(|| Feed::new(code.clone()))
                .clone()
        };

        let prev = feed.subscribers.fetch_add(1, Ordering::SeqCst);
        println!(
            "RealtimePrice {}: subscribe (total {})",
            feed.code,
            prev + 1
        );

        let mut last = feed.last.lock().await.clone();
        if last.is_none() {
            if let Some(cached) = redis_cache::get(&feed.code).await {
                {
                    let mut slot = feed.last.lock().await;
                    *slot = Some(cached.clone());
                }
                last = Some(cached);
            }
        }

        if prev == 0 {
            feed.ensure_poller().await;
        }

        let rx = feed.tx.subscribe();
        Subscription { feed, rx, last }
    }
}
