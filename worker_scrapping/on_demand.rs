//! On-demand scrape Stockbit: portofolio, pending_order, buy limit order, emiten_trending,
//! portofolio_history.

use std::collections::HashMap;
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

static INFLIGHT_PORTO_HISTORY: OnceLock<
    Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>>,
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

fn inflight_porto_history(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PORTO_HISTORY.get_or_init(|| Mutex::new(HashMap::new()))
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
    // On-demand RPC: tanpa cek jam operasional (batas hari/jam hanya di poller).
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
    // On-demand RPC: tanpa cek jam operasional (batas hari/jam hanya di poller).
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
    // On-demand RPC: tanpa cek jam operasional (batas hari/jam hanya di poller).
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

    println!("On-demand: emiten_trending via market-mover API (Top Gainer/Loser)...");

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

/// Senin–Jumat, jam 09:00–12:15 dan 13:30–16:15 (waktu server lokal).
pub fn is_stockbit_poller_scrape_hours() -> bool {
    use chrono::{Datelike, Local, Timelike};

    let now = Local::now();
    match now.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => return false,
        _ => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 9 * 60;
    const MORNING_END: u32 = 12 * 60 + 15 + 1;
    const AFTERNOON_START: u32 = 13 * 60 + 30;
    const AFTERNOON_END: u32 = 16 * 60 + 15 + 1;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    in_morning || in_afternoon
}

/// On-demand: login → PIN → GET carina `/history?stock=` →
/// upsert `portofolio_history` per tanggal. Single-flight per emiten.
/// Returns jumlah entri history yang di-upsert.
pub async fn scrape_portofolio_history_for_emiten(
    session: Arc<Session>,
    emiten_name: &str,
) -> Result<usize, String> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet (contoh: ASBI)".into());
    }

    let mut rx = {
        let mut map = inflight_porto_history().lock().await;
        if let Some(existing) = map.get(&code) {
            println!("On-demand portofolio history: {code} sudah berjalan — menunggu hasil...");
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            map.insert(code.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            tokio::spawn(async move {
                let result = run_portofolio_history_scrape(session, &code_spawn).await;
                match &result {
                    Ok(n) => println!("On-demand portofolio history selesai {code_spawn}: {n} entri."),
                    Err(e) => {
                        eprintln!("On-demand portofolio history GAGAL {code_spawn}: {e}")
                    }
                }
                let _ = tx.send(Some(result));
                inflight_porto_history().lock().await.remove(&code_spawn);
            });
            rx
        }
    };

    loop {
        {
            let guard = rx.borrow();
            if let Some(result) = guard.as_ref() {
                return result.clone();
            }
        }
        if rx.changed().await.is_err() {
            return Err(format!(
                "on-demand portofolio history {code}: channel ditutup sebelum ada hasil"
            ));
        }
    }
}

async fn run_portofolio_history_scrape(
    session: Arc<Session>,
    code: &str,
) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL").map_err(|_| {
        "STOCKBIT_EMAIL wajib diisi untuk scrape portofolio history".to_string()
    })?;
    let password = std::env::var("STOCKBIT_PASSWORD").map_err(|_| {
        "STOCKBIT_PASSWORD wajib diisi untuk scrape portofolio history".to_string()
    })?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let ks = keyspace();

    println!("On-demand portofolio history: login → PIN → /history {code}...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let n = crate::portofolio_history_worker::scrape_and_replace_portofolio_history(
            &page,
            session.as_ref(),
            &ks,
            code,
        )
        .await
        .map(|(n, _, _)| n)
        .map_err(|e| e.to_string())?;

        Ok::<usize, String>(n)
    }
    .await;

    browser.close().await;
    result
}

/// Dipanggil dari readiness poller setelah tiap tick: scrape portofolio, emiten_trending,
/// pending_order — hanya Senin–Jumat 09:00–12:15 & 13:30–16:15, dan hanya bila `ready`.
/// Urutan: (1) portofolio serial; lalu paralel Yahoo ATR + trending + pending_order.
/// `None` = skip (jangan overwrite cache portofolio); `Some` = hasil cek Yahoo (bisa kosong).
pub async fn run_poller_stockbit_scrapes(
    session: Arc<Session>,
    ready: bool,
) -> Option<Vec<stockbit_browser::PortofolioSpike>> {
    if !ready {
        println!("Poller scrapes: Stockbit belum ready — skip");
        return None;
    }
    if !is_stockbit_poller_scrape_hours() {
        println!(
            "Poller scrapes: diluar jam operasional Senin–Jumat 09:00–12:15 & 13:30–16:15 — skip"
        );
        return None;
    }

    println!("Poller scrapes: mulai GetAllPortofolioFromStockbit (serial)");

    match run_portofolio_all_scrape(Arc::clone(&session), BrowserLockClass::Background, false)
        .await
    {
        Ok((n, _)) => println!("Poller GetAllPortofolioFromStockbit OK: {n} holdings"),
        Err(e) => eprintln!("Poller GetAllPortofolioFromStockbit skip/fail: {e}"),
    }

    println!(
        "Poller scrapes: paralel Yahoo ATR || (GetLatestEmitenTrendingFromStockbit → GetAllPendingOrderFromStockbit)"
    );

    let session_yahoo = Arc::clone(&session);
    let session_stockbit = Arc::clone(&session);

    let (spikes, ()) = tokio::join!(
        async move {
            let emitens = match list_portofolio_emiten_names(session_yahoo.as_ref()).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Poller Yahoo ATR: gagal baca portofolio: {e}");
                    return Vec::new();
                }
            };

            let reported = crate::yahoo_spike_cache::already_reported().await;
            let to_check: Vec<String> = emitens
                .into_iter()
                .filter(|e| !reported.contains(e))
                .collect();
            let skipped = reported.len();
            println!(
                "\x1b[32mPoller Yahoo ATR: cek {} emiten (skip {} sudah di-output hari ini)\x1b[0m",
                to_check.len(),
                skipped
            );

            let spikes = crate::yahoo_atr::find_spike_emitens(&to_check).await;
            if spikes.is_empty() {
                println!("Poller Yahoo spike: tidak ada lonjakan baru (UP >= 16% / DOWN >= 8% vs open)");
            } else {
                let summary: Vec<String> = spikes
                    .iter()
                    .map(|s| {
                        format!(
                            "{}:{}:{:+.2}%",
                            s.emiten_name, s.jenis_spike, s.value_spike_percentage
                        )
                    })
                    .collect();
                println!(
                    "\x1b[32mPoller Yahoo ATR: lonjakan baru {}\x1b[0m",
                    summary.join(", ")
                );
                let names: Vec<String> =
                    spikes.iter().map(|s| s.emiten_name.clone()).collect();
                crate::yahoo_spike_cache::mark_reported(&names).await;
            }
            spikes
                .into_iter()
                .map(|s| stockbit_browser::PortofolioSpike {
                    emiten_name: s.emiten_name,
                    jenis_spike: s.jenis_spike,
                    value_spike_percentage: s.value_spike_percentage,
                })
                .collect()
        },
        async move {
            // Trending lalu pending serial (satu Chrome); berjalan bersamaan dengan Yahoo.
            match run_emiten_trending_movers_scrape(
                Arc::clone(&session_stockbit),
                BrowserLockClass::Background,
            )
            .await
            {
                Ok((g, l)) => {
                    println!(
                        "Poller GetLatestEmitenTrendingFromStockbit OK: gainer={g} loser={l}"
                    )
                }
                Err(e) => {
                    eprintln!("Poller GetLatestEmitenTrendingFromStockbit skip/fail: {e}")
                }
            }

            match run_pending_order_all_scrape(session_stockbit, BrowserLockClass::Background)
                .await
            {
                Ok(n) => println!("Poller GetAllPendingOrderFromStockbit OK: {n} baris"),
                Err(e) => eprintln!("Poller GetAllPendingOrderFromStockbit skip/fail: {e}"),
            }
        },
    );

    Some(spikes)
}

async fn list_portofolio_emiten_names(session: &Session) -> Result<Vec<String>, String> {
    use futures_util::TryStreamExt;
    use scylla::DeserializeRow;

    #[derive(Debug, DeserializeRow)]
    struct Row {
        emiten_name: String,
    }

    let mut stream = session
        .query_iter("SELECT emiten_name FROM invezgood.portofolio", &[])
        .await
        .map_err(|e| format!("SELECT portofolio emiten_name: {e}"))?
        .rows_stream::<Row>()
        .map_err(|e| format!("portofolio stream: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = stream
        .try_next()
        .await
        .map_err(|e| format!("portofolio row: {e}"))?
    {
        let code = row.emiten_name.trim().to_ascii_uppercase();
        if !code.is_empty() {
            out.push(code);
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}
