//! On-demand scrape Stockbit: portofolio, pending_order, buy limit order, emiten_trending.

use std::sync::{Arc, OnceLock};

use scylla::client::session::Session;
use stockbit_browser::{
    acquire_browser_session, launch_page, open_stream_or_login, BrowserLockClass,
};
use tokio::sync::{Mutex, watch};

static INFLIGHT_PORTO_ALL: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<(usize, Vec<String>), String>>>>>,
> = OnceLock::new();

static INFLIGHT_PENDING_ORDER: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_BUY_LIMIT: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<(), String>>>>>,
> = OnceLock::new();

static INFLIGHT_EMITEN_TRENDING: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<(usize, usize), String>>>>>,
> = OnceLock::new();

fn inflight_porto_all(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<(usize, Vec<String>), String>>>>> {
    INFLIGHT_PORTO_ALL.get_or_init(|| Mutex::new(None))
}

fn inflight_pending_order(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PENDING_ORDER.get_or_init(|| Mutex::new(None))
}

fn inflight_buy_limit() -> &'static Mutex<Option<watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_BUY_LIMIT.get_or_init(|| Mutex::new(None))
}

fn inflight_emiten_trending(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<(usize, usize), String>>>>> {
    INFLIGHT_EMITEN_TRENDING.get_or_init(|| Mutex::new(None))
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "invezgood".to_string())
}

async fn wait_watch<T: Clone>(mut rx: watch::Receiver<Option<T>>, label: &str) -> Result<T, String> {
    loop {
        {
            let guard = rx.borrow();
            if let Some(result) = guard.as_ref() {
                return Ok(result.clone());
            }
        }
        if rx.changed().await.is_err() {
            return Err(format!("{label}: channel ditutup sebelum ada hasil"));
        }
    }
}

pub async fn scrape_portofolio_all(
    session: Arc<Session>,
) -> Result<(usize, Vec<String>), String> {
    let rx = {
        let mut slot = inflight_porto_all().lock().await;
        if let Some(existing) = slot.as_ref() {
            existing.clone()
        } else {
            let (tx, rx) =
                watch::channel::<Option<Result<(usize, Vec<String>), String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result =
                    run_portofolio_all_scrape(session, BrowserLockClass::Interactive, false).await;
                let _ = tx.send(Some(result));
                *inflight_porto_all().lock().await = None;
            });
            rx
        }
    };
    wait_watch(rx, "on-demand portofolio").await?
}

async fn run_portofolio_all_scrape(
    session: Arc<Session>,
    lock_class: BrowserLockClass,
    with_bandarmology: bool,
) -> Result<(usize, Vec<String>), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape portofolio".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape portofolio".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = acquire_browser_session(lock_class).await?;
    let ks = keyspace();

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let (n, codes) = crate::portofolio_worker::scrape_and_insert_portofolio(
            &page,
            &session,
            &ks,
            with_bandarmology,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<(usize, Vec<String>), String>((n, codes))
    }
    .await;

    browser.close().await;
    result
}

pub async fn scrape_pending_order_all(session: Arc<Session>) -> Result<usize, String> {
    let rx = {
        let mut slot = inflight_pending_order().lock().await;
        if let Some(existing) = slot.as_ref() {
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result =
                    run_pending_order_all_scrape(session, BrowserLockClass::Interactive).await;
                let _ = tx.send(Some(result));
                *inflight_pending_order().lock().await = None;
            });
            rx
        }
    };
    wait_watch(rx, "on-demand pending_order").await?
}

async fn run_pending_order_all_scrape(
    session: Arc<Session>,
    lock_class: BrowserLockClass,
) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape pending_order".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape pending_order".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = acquire_browser_session(lock_class).await?;
    let ks = keyspace();

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let n = crate::pending_order_worker::scrape_and_insert_pending_order(
            &page, &session, &ks,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<usize, String>(n)
    }
    .await;

    browser.close().await;
    result
}

pub async fn create_buy_limit_order(
    emiten_name: String,
    price: i32,
    lot: i32,
    expiry_dom_value: String,
) -> Result<(), String> {
    let rx = {
        let mut slot = inflight_buy_limit().lock().await;
        if let Some(existing) = slot.as_ref() {
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<(), String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result =
                    run_create_buy_limit_order(emiten_name, price, lot, expiry_dom_value).await;
                let _ = tx.send(Some(result));
                *inflight_buy_limit().lock().await = None;
            });
            rx
        }
    };
    wait_watch(rx, "on-demand buy_limit").await?
}

async fn run_create_buy_limit_order(
    emiten_name: String,
    price: i32,
    lot: i32,
    expiry_dom_value: String,
) -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk CreateBuyLimitOrder".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk CreateBuyLimitOrder".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = acquire_browser_session(BrowserLockClass::Interactive).await?;

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        crate::buy_limit_order_worker::create_buy_limit_order(
            &page,
            &emiten_name,
            price,
            lot,
            &expiry_dom_value,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<(), String>(())
    }
    .await;

    browser.close().await;
    result
}

pub async fn scrape_emiten_trending_movers(
    session: Arc<Session>,
) -> Result<(usize, usize), String> {
    let rx = {
        let mut slot = inflight_emiten_trending().lock().await;
        if let Some(existing) = slot.as_ref() {
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<(usize, usize), String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result = run_emiten_trending_movers_scrape(
                    session,
                    BrowserLockClass::Interactive,
                )
                .await;
                let _ = tx.send(Some(result));
                *inflight_emiten_trending().lock().await = None;
            });
            rx
        }
    };
    wait_watch(rx, "on-demand emiten_trending").await?
}

async fn run_emiten_trending_movers_scrape(
    session: Arc<Session>,
    lock_class: BrowserLockClass,
) -> Result<(usize, usize), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape movers".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape movers".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = acquire_browser_session(lock_class).await?;
    let ks = keyspace();

    println!(
        "\x1b[32mOn-demand: emiten_trending via market-mover API (Top Gainer/Loser)...\x1b[0m"
    );

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let (gainer, loser) = crate::emiten_trending_worker::scrape_and_insert_movers(
            &page,
            session.as_ref(),
            &ks,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<(usize, usize), String>((gainer, loser))
    }
    .await;

    browser.close().await;
    result
}

/// Senin–Jumat, jam 09:00–12:00 dan 13:30–16:00 (waktu server lokal).
pub fn is_stockbit_poller_scrape_hours() -> bool {
    use chrono::{Datelike, Local, Timelike};

    let now = Local::now();
    match now.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => return false,
        _ => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 9 * 60;
    const MORNING_END: u32 = 12 * 60 + 1;
    const AFTERNOON_START: u32 = 13 * 60 + 30;
    const AFTERNOON_END: u32 = 16 * 60 + 1;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    in_morning || in_afternoon
}

/// Dipanggil dari readiness poller setelah tiap tick: scrape portofolio, emiten_trending,
/// pending_order — hanya Senin–Jumat 09:00–12:00 & 13:30–16:00, dan hanya bila `ready`.
pub async fn run_poller_stockbit_scrapes(session: Arc<Session>, ready: bool) {
    if !ready {
        println!("Poller scrapes: Stockbit belum ready — skip");
        return;
    }
    if !is_stockbit_poller_scrape_hours() {
        println!(
            "Poller scrapes: diluar jam operasional Senin–Jumat 09:00–12:00 & 13:30–16:00 — skip"
        );
        return;
    }

    println!(
        "Poller scrapes: mulai GetAllPortofolioFromStockbit + GetLatestEmitenTrendingFromStockbit + GetAllPendingOrderFromStockbit"
    );

    match run_portofolio_all_scrape(Arc::clone(&session), BrowserLockClass::Background, false)
        .await
    {
        Ok((n, _)) => println!("Poller GetAllPortofolioFromStockbit OK: {n} holdings"),
        Err(e) => eprintln!("Poller GetAllPortofolioFromStockbit skip/fail: {e}"),
    }

    match run_emiten_trending_movers_scrape(Arc::clone(&session), BrowserLockClass::Background)
        .await
    {
        Ok((g, l)) => {
            println!("Poller GetLatestEmitenTrendingFromStockbit OK: gainer={g} loser={l}")
        }
        Err(e) => eprintln!("Poller GetLatestEmitenTrendingFromStockbit skip/fail: {e}"),
    }

    match run_pending_order_all_scrape(session, BrowserLockClass::Background).await {
        Ok(n) => println!("Poller GetAllPendingOrderFromStockbit OK: {n} baris"),
        Err(e) => eprintln!("Poller GetAllPendingOrderFromStockbit skip/fail: {e}"),
    }
}
