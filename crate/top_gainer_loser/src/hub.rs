use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session::Session;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

use crate::model::TopGainerLoserRow;

const POLL_INTERVAL: Duration = Duration::from_secs(15 * 60);
const BROADCAST_CAPACITY: usize = 32;

pub struct TodayPollHub {
    session: Arc<Session>,
    subscribers: AtomicUsize,
    tx: broadcast::Sender<Vec<TopGainerLoserRow>>,
    poll_task: Mutex<Option<JoinHandle<()>>>,
    last_snapshot: Mutex<Option<Vec<TopGainerLoserRow>>>,
}

impl TodayPollHub {
    pub fn new(session: Arc<Session>) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            session,
            subscribers: AtomicUsize::new(0),
            tx,
            poll_task: Mutex::new(None),
            last_snapshot: Mutex::new(None),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<TopGainerLoserRow>> {
        self.tx.subscribe()
    }

    pub async fn last_snapshot(&self) -> Option<Vec<TopGainerLoserRow>> {
        self.last_snapshot.lock().await.clone()
    }

    pub async fn add_subscriber(self: &Arc<Self>) {
        let count = self.subscribers.fetch_add(1, Ordering::SeqCst) + 1;
        if count == 1 {
            self.start_poller().await;
        }
    }

    pub async fn remove_subscriber(self: &Arc<Self>) {
        let count = self.subscribers.fetch_sub(1, Ordering::SeqCst) - 1;
        if count == 0 {
            *self.last_snapshot.lock().await = None;
            self.stop_poller().await;
        }
    }

    async fn start_poller(self: &Arc<Self>) {
        let mut guard = self.poll_task.lock().await;
        if guard.is_some() {
            return;
        }

        let hub = Arc::clone(self);
        *guard = Some(tokio::spawn(async move {
            hub.run_poller().await;
        }));
    }

    async fn stop_poller(&self) {
        if let Some(handle) = self.poll_task.lock().await.take() {
            handle.abort();
        }
    }

    async fn run_poller(self: Arc<Self>) {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if self.subscribers.load(Ordering::SeqCst) == 0 {
                break;
            }

            let today = chrono::Local::now().date_naive();
            match crate::invezgo::fetch_and_save(self.session.clone(), today).await {
                Ok(rows) => {
                    *self.last_snapshot.lock().await = Some(rows.clone());
                    let _ = self.tx.send(rows);
                }
                Err(error) => eprintln!("top_gainer_loser poll Invezgo gagal: {error}"),
            }
        }

        let _ = self.poll_task.lock().await.take();
    }
}

/// Lepas subscriber saat stream client putus.
pub struct SubscriberGuard {
    hub: Arc<TodayPollHub>,
}

impl SubscriberGuard {
    pub fn new(hub: Arc<TodayPollHub>) -> Self {
        Self { hub }
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        let hub = Arc::clone(&self.hub);
        tokio::spawn(async move {
            hub.remove_subscriber().await;
        });
    }
}
