//! Poller per `emiten_name`: GET Stockbit 1×/menit hanya jika ada subscriber.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::fetch::fetch_realtime_price;
use crate::GetRealtimePriceFromStockbitResponse;

const POLL_INTERVAL: Duration = Duration::from_secs(60);
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

    async fn run_poller(self: Arc<Self>) {
        println!(
            "RealtimePrice poller {}: start (subscriber aktif)",
            self.code
        );
        while self.subscribers.load(Ordering::SeqCst) > 0 {
            println!(
                "RealtimePrice scrape emiten_name={}: GET emitten/{}/info ...",
                self.code, self.code
            );
            match stockbit_browser::ensure_stockbit_bearer().await {
                Ok(bearer) => match fetch_realtime_price(&self.code, &bearer).await {
                    Ok(resp) => {
                        println!(
                            "RealtimePrice scrape emiten_name={} OK: symbol={} price={} formatted_price={} time={} volume={}",
                            self.code,
                            resp.symbol,
                            resp.price,
                            resp.formatted_price,
                            resp.time,
                            resp.volume
                        );
                        {
                            let mut last = self.last.lock().await;
                            *last = Some(resp.clone());
                        }
                        let _ = self.tx.send(resp);
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

            // Sleep 60s dalam potongan 1s — berhenti segera bila tidak ada subscriber.
            for _ in 0..POLL_INTERVAL.as_secs() {
                if self.subscribers.load(Ordering::SeqCst) == 0 {
                    println!(
                        "RealtimePrice poller {}: stop (tidak ada subscriber) — tidak GET API lagi",
                        self.code
                    );
                    return;
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
        println!(
            "RealtimePrice poller {}: stop (tidak ada subscriber)",
            self.code
        );
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
        // Poller mendeteksi subscribers==0 lalu berhenti sendiri (tanpa GET lagi).
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
        if prev == 0 {
            feed.ensure_poller().await;
        }

        let last = feed.last.lock().await.clone();
        let rx = feed.tx.subscribe();
        Subscription { feed, rx, last }
    }
}
