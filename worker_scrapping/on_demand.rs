//! On-demand scrape Stockbit (movers, emiten_list, bandarmology, portofolio, pending_order, equity, history).
//!
//! Scrape dijalankan di `tokio::spawn` + single-flight per `emiten_name`, supaya
//! cancel/timeout di sisi gRPC client **tidak** membatalkan scrape yang sedang jalan
//! (pola log: login OK → client retry berulang sebelum bearer warm-up selesai).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Local, NaiveDate};
use scylla::client::session::Session;
use stockbit_browser::{
    browser_session_lock, launch_page, open_stream_or_login,
};
use tokio::sync::{Mutex, watch};

use crate::{bandarmology_worker, emiten_list_worker};

static INFLIGHT_EMITEN: OnceLock<Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>>> =
    OnceLock::new();

static INFLIGHT_EMITEN_STOCKBIT: OnceLock<
    Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>>,
> = OnceLock::new();


static INFLIGHT_BANDAR_HARIAN: OnceLock<
    Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_PORTO_EQUITY: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_PORTO_HISTORY: OnceLock<
    Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_PORTO_ALL: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<(usize, Vec<String>), String>>>>>,
> = OnceLock::new();

static INFLIGHT_PORTO_HISTORY_BATCH: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_PENDING_ORDER: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

static INFLIGHT_BUY_LIMIT: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<(), String>>>>>,
> = OnceLock::new();

static INFLIGHT_IDX30: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>>,
> = OnceLock::new();

static INFLIGHT_LQ45: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>>,
> = OnceLock::new();

static INFLIGHT_IDX80: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>>,
> = OnceLock::new();

static INFLIGHT_KOMPAS100: OnceLock<
    Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>>,
> = OnceLock::new();

fn inflight_map() -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_EMITEN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn inflight_stockbit_map(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_EMITEN_STOCKBIT.get_or_init(|| Mutex::new(HashMap::new()))
}


fn inflight_bandar_harian_map(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_BANDAR_HARIAN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn inflight_porto_equity(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PORTO_EQUITY.get_or_init(|| Mutex::new(None))
}

fn inflight_porto_history(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PORTO_HISTORY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn inflight_porto_all(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<(usize, Vec<String>), String>>>>> {
    INFLIGHT_PORTO_ALL.get_or_init(|| Mutex::new(None))
}

fn inflight_porto_history_batch(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PORTO_HISTORY_BATCH.get_or_init(|| Mutex::new(None))
}

fn inflight_pending_order(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_PENDING_ORDER.get_or_init(|| Mutex::new(None))
}

fn inflight_buy_limit() -> &'static Mutex<Option<watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_BUY_LIMIT.get_or_init(|| Mutex::new(None))
}

fn inflight_idx30(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>> {
    INFLIGHT_IDX30.get_or_init(|| Mutex::new(None))
}

fn inflight_lq45(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>> {
    INFLIGHT_LQ45.get_or_init(|| Mutex::new(None))
}

fn inflight_idx80(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>> {
    INFLIGHT_IDX80.get_or_init(|| Mutex::new(None))
}

fn inflight_kompas100(
) -> &'static Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>> {
    INFLIGHT_KOMPAS100.get_or_init(|| Mutex::new(None))
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}

/// Bila `emiten_list` belum ada, atau `update_at` stale (≥30 hari): Key Stats + Corp.Action
/// + Profile API → upsert `emiten_list`.
/// Lalu bila `bandarmology` agg bulan ini (`YYYY-MM_CODE`) belum ada: scrape bandarmology.
///
/// Aman terhadap cancel RPC: scrape tetap jalan di background; panggilan berikutnya
/// untuk code yang sama menunggu hasil yang sama.
pub async fn ensure_emiten_data_for_code(
    session: Arc<Session>,
    emiten_name: &str,
) -> Result<(), String> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("emiten_name kosong".into());
    }

    let mut rx = {
        let mut map = inflight_map().lock().await;
        if let Some(existing) = map.get(&code) {
            println!("On-demand: {code} sudah berjalan — menunggu hasil (single-flight)...");
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<(), String>>>(None);
            map.insert(code.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            tokio::spawn(async move {
                let result = run_ensure_emiten_scrape(session, &code_spawn).await;
                match &result {
                    Ok(()) => println!("On-demand scrape selesai untuk {code_spawn}."),
                    Err(e) => eprintln!("On-demand scrape GAGAL {code_spawn}: {e}"),
                }
                let _ = tx.send(Some(result));
                inflight_map().lock().await.remove(&code_spawn);
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
                "on-demand scrape {code}: channel ditutup sebelum ada hasil"
            ));
        }
    }
}

async fn run_ensure_emiten_scrape(
    session: Arc<Session>,
    code: &str,
) -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk on-demand scrape".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk on-demand scrape".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();
    let today = Local::now().date_naive();

    println!("On-demand scrape: mulai untuk {code} (emiten_list + bandarmology)...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;
        // open_stream_or_login sudah dismiss modal bila ada — jangan loop 8× lagi
        // (membuang ~4s; sering kena timeout client gRPC ~10s).

        println!("On-demand: ambil bearer + API keystats/corpaction/profile untuk {code}...");
        emiten_list_worker::scrape_emiten_list_for_code(&page, session.as_ref(), &ks, code)
            .await?;

        println!("On-demand: bandarmology API untuk {code}...");
        bandarmology_worker::scrape_bandarmology_for_code_if_missing(
            &page,
            session,
            &ks,
            today,
            code,
        )
        .await?;

        Ok::<(), String>(())
    }
    .await;

    browser.close().await;

    result
}

/// Scrape Key Stats + Corp.Action + Profile dari Stockbit API untuk satu `emiten_name`
/// (tanpa cek Scylla / `update_at`); upsert `emiten_list`.
/// Bila `also_bandarmology`: setelah emiten_list, scrape API bandarmology untuk kode tersebut.
pub async fn scrape_emiten_list_from_stockbit_for_code(
    session: Arc<Session>,
    emiten_name: &str,
    also_bandarmology: bool,
) -> Result<(), String> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("emiten_name kosong".into());
    }

    let mut rx = {
        let mut map = inflight_stockbit_map().lock().await;
        if let Some(existing) = map.get(&code) {
            println!(
                "On-demand Stockbit: {code} sudah berjalan — menunggu hasil (single-flight)..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<(), String>>>(None);
            map.insert(code.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            let also_bandarmology_spawn = also_bandarmology;
            tokio::spawn(async move {
                let result =
                    run_emiten_list_stockbit_scrape(session, &code_spawn, also_bandarmology_spawn)
                        .await;
                match &result {
                    Ok(()) => println!("On-demand Stockbit scrape selesai untuk {code_spawn}."),
                    Err(e) => eprintln!("On-demand Stockbit scrape GAGAL {code_spawn}: {e}"),
                }
                let _ = tx.send(Some(result));
                inflight_stockbit_map().lock().await.remove(&code_spawn);
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
                "on-demand Stockbit scrape {code}: channel ditutup sebelum ada hasil"
            ));
        }
    }
}

async fn run_emiten_list_stockbit_scrape(
    session: Arc<Session>,
    code: &str,
    also_bandarmology: bool,
) -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk on-demand scrape".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk on-demand scrape".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();
    let today = Local::now().date_naive();

    let scope = if also_bandarmology {
        "emiten_list + bandarmology"
    } else {
        "emiten_list saja"
    };
    println!("On-demand Stockbit scrape: mulai untuk {code} ({scope})...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        println!("On-demand Stockbit: ambil bearer + API keystats/corpaction/profile untuk {code}...");
        emiten_list_worker::scrape_emiten_list_for_code(&page, session.as_ref(), &ks, code)
            .await?;

        if also_bandarmology {
            println!("On-demand Stockbit: bandarmology API untuk {code}...");
            bandarmology_worker::scrape_bandarmology_for_code_if_missing(
                &page,
                session,
                &ks,
                today,
                code,
            )
            .await?;
        }

        Ok::<(), String>(())
    }
    .await;

    browser.close().await;

    result
}

/// On-demand scrape Top Gainer/Loser (movers) → upsert `emiten_trending` + seed `emiten_list`;
/// lalu baca `emiten_name` dari `emiten_list` (setelah seed) → keystats/profile/corpaction.
/// Tidak scrape / tulis `bandarmology`.
pub async fn scrape_emiten_trending_movers(
    session: Arc<Session>,
) -> Result<(usize, usize), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape movers".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape movers".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!("On-demand: emiten_trending via market-mover API (Top Gainer/Loser)...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let (inserted_gainer, inserted_loser, mover_codes) =
            crate::emiten_trending_worker::scrape_and_insert_movers(
                &page,
                session.as_ref(),
                &ks,
            )
            .await
            .map_err(|e| e.to_string())?;

        // Setelah seed movers → baca ulang emiten_list (sudah termasuk ticker baru).
        println!("On-demand: token-ring scan emiten_list.emiten_name (setelah seed movers)...");
        let existing = bandarmology_worker::fetch_emiten_list_emiten_names(session.as_ref(), &ks)
            .await
            .map_err(|e| e.to_string())?;
        let emitens = merge_codes_movers_first(&existing, &mover_codes);
        println!(
            "On-demand: {} emiten untuk key_stats/profile/corp (movers dulu={}, scan={}).",
            emitens.len(),
            mover_codes.len(),
            existing.len()
        );

        let key_stats_ok = emiten_list_worker::scrape_and_insert_key_stats(
            &page,
            session.as_ref(),
            &ks,
            &emitens,
        )
        .await
        .map_err(|e| e.to_string())?;
        println!("On-demand: {key_stats_ok} emiten key_stats/profile diupsert ke emiten_list.");

        Ok::<(usize, usize), String>((inserted_gainer, inserted_loser))
    }
    .await;

    browser.close().await;

    result
}

/// `mover_codes` di depan, lalu sisa `existing` tanpa duplikat.
fn merge_codes_movers_first(existing: &[String], mover_codes: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(existing.len() + mover_codes.len());
    let mut seen = std::collections::HashSet::new();
    for c in mover_codes {
        let code = c.trim().to_ascii_uppercase();
        if code.is_empty() || !seen.insert(code.clone()) {
            continue;
        }
        out.push(code);
    }
    for c in existing {
        let code = c.trim().to_ascii_uppercase();
        if code.is_empty() || !seen.insert(code.clone()) {
            continue;
        }
        out.push(code);
    }
    out
}

/// Scrape bandarmology harian untuk daftar tanggal.
/// Worker skip tanggal yang sudah non-empty di Scylla; timpa bila `broker_summary_harian` kosong.
/// Single-flight per `(emiten + sorted days)`; survive cancel RPC.
/// Returns jumlah hari yang di-upsert.
pub async fn scrape_bandarmology_harian_days_from_stockbit(
    session: Arc<Session>,
    emiten_name: &str,
    days: &[NaiveDate],
) -> Result<usize, String> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("emiten_name kosong".into());
    }
    if days.is_empty() {
        return Ok(0);
    }

    let mut sorted = days.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let flight_key = format!(
        "{code}:{}",
        sorted
            .iter()
            .map(|d| d.format("%Y-%m-%d").to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    let mut rx = {
        let mut map = inflight_bandar_harian_map().lock().await;
        if let Some(existing) = map.get(&flight_key) {
            println!(
                "On-demand bandarmology_harian: {flight_key} sudah berjalan — menunggu hasil..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            map.insert(flight_key.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            let key_spawn = flight_key.clone();
            let days_spawn = sorted.clone();
            tokio::spawn(async move {
                let result =
                    run_bandarmology_harian_days(session, &code_spawn, &days_spawn).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand bandarmology_harian selesai {key_spawn}: {n} hari di-upsert."
                    ),
                    Err(e) => eprintln!(
                        "On-demand bandarmology_harian GAGAL {key_spawn}: {e}"
                    ),
                }
                let _ = tx.send(Some(result));
                inflight_bandar_harian_map().lock().await.remove(&key_spawn);
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
                "on-demand bandarmology_harian {flight_key}: channel ditutup sebelum ada hasil"
            ));
        }
    }
}

async fn run_bandarmology_harian_days(
    session: Arc<Session>,
    code: &str,
    days: &[NaiveDate],
) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape bandarmology_harian".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape bandarmology_harian".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!(
        "On-demand bandarmology_harian: mulai untuk {code} ({} hari)...",
        days.len()
    );

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let n = bandarmology_worker::scrape_and_upsert_bandarmology_harian_days(
            &page,
            session.as_ref(),
            &ks,
            code,
            days,
        )
        .await?;

        Ok::<usize, String>(n)
    }
    .await;

    browser.close().await;

    result
}

/// On-demand scrape equity portfolio dari API `portfolio/v2/list` (`data.summary`)
/// → upsert `portofolio_equity`.
/// Alur: login → START TRADING/PIN bila perlu → Bearer trading → GET API → insert.
/// Single-flight global; survive cancel RPC.
/// Returns jumlah baris yang di-upsert.
pub async fn scrape_portofolio_equity(session: Arc<Session>) -> Result<usize, String> {
    let mut rx = {
        let mut slot = inflight_porto_equity().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!(
                "On-demand portofolio_equity: sudah berjalan — menunggu hasil (single-flight)..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            *slot = Some(rx.clone());
            let session = Arc::clone(&session);
            tokio::spawn(async move {
                let result = run_portofolio_equity_scrape(session).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand portofolio_equity selesai: {n} baris di-upsert."
                    ),
                    Err(e) => eprintln!("On-demand portofolio_equity GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight_porto_equity().lock().await = None;
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
            return Err(
                "on-demand portofolio_equity: channel ditutup sebelum ada hasil".into(),
            );
        }
    }
}

async fn run_portofolio_equity_scrape(session: Arc<Session>) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape portofolio_equity".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape portofolio_equity".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!("On-demand portofolio_equity: login → PIN → portfolio/v2/list summary...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let n = crate::portofolio_worker::scrape_and_insert_portofolio_equity(
            &page,
            session.as_ref(),
            &ks,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<usize, String>(n)
    }
    .await;

    browser.close().await;

    result
}

/// On-demand: login → PIN → GET carina `/history?stock=` →
/// upsert `portofolio_history` per tanggal transaksi. Single-flight per emiten; survive cancel RPC.
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
            println!(
                "On-demand portofolio history: {code} sudah berjalan — menunggu hasil..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            map.insert(code.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            tokio::spawn(async move {
                let result = run_portofolio_history_scrape(session, &code_spawn).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand portofolio history selesai {code_spawn}: {n} entri."
                    ),
                    Err(e) => eprintln!(
                        "On-demand portofolio history GAGAL {code_spawn}: {e}"
                    ),
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
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape portofolio history".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape portofolio history".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
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

/// On-demand scrape semua holdings portfolio → upsert `portofolio`
/// (alur `portofolio_worker::scrape_and_insert_portofolio`; tidak tulis `portofolio_bandarmology`).
/// Single-flight global; survive cancel RPC.
/// Returns `(baris_upsert, kode_holding)`.
pub async fn scrape_portofolio_all(
    session: Arc<Session>,
) -> Result<(usize, Vec<String>), String> {
    let mut rx = {
        let mut slot = inflight_porto_all().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!(
                "On-demand portofolio: sudah berjalan — menunggu hasil (single-flight)..."
            );
            existing.clone()
        } else {
            let (tx, rx) =
                watch::channel::<Option<Result<(usize, Vec<String>), String>>>(None);
            *slot = Some(rx.clone());
            let session = Arc::clone(&session);
            tokio::spawn(async move {
                let result = run_portofolio_all_scrape(session).await;
                match &result {
                    Ok((n, codes)) => println!(
                        "On-demand portofolio selesai: {n} baris di-upsert ({} kode).",
                        codes.len()
                    ),
                    Err(e) => eprintln!("On-demand portofolio GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight_porto_all().lock().await = None;
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
            return Err("on-demand portofolio: channel ditutup sebelum ada hasil".into());
        }
    }
}

async fn run_portofolio_all_scrape(
    session: Arc<Session>,
) -> Result<(usize, Vec<String>), String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape portofolio".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape portofolio".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!("On-demand portofolio: login → PIN → portfolio/v2/list (summary + results)...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let (n, codes) =
            crate::portofolio_worker::scrape_and_insert_portofolio(&page, &session, &ks, true)
                .await
                .map_err(|e| e.to_string())?;

        Ok::<(usize, Vec<String>), String>((n, codes))
    }
    .await;

    browser.close().await;

    result
}

/// On-demand batch: history order per emiten holdings (satu sesi browser),
/// alur sama worker `scrape_and_upsert_portofolio_history_for_emitens`.
/// Single-flight global. Returns jumlah emiten yang berhasil di-upsert.
pub async fn scrape_portofolio_history_for_emitens(
    session: Arc<Session>,
    emitens: &[String],
) -> Result<usize, String> {
    let codes: Vec<String> = emitens
        .iter()
        .map(|c| c.trim().to_ascii_uppercase())
        .filter(|c| c.len() == 4 && c.chars().all(|ch| ch.is_ascii_alphabetic()))
        .collect();
    if codes.is_empty() {
        return Ok(0);
    }

    let mut rx = {
        let mut slot = inflight_porto_history_batch().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!(
                "On-demand portofolio history batch: sudah berjalan — menunggu hasil..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            *slot = Some(rx.clone());
            let session = Arc::clone(&session);
            let codes_spawn = codes.clone();
            tokio::spawn(async move {
                let result =
                    run_portofolio_history_batch_scrape(session, codes_spawn).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand portofolio history batch selesai: {n} emiten di-upsert."
                    ),
                    Err(e) => eprintln!("On-demand portofolio history batch GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight_porto_history_batch().lock().await = None;
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
            return Err(
                "on-demand portofolio history batch: channel ditutup sebelum ada hasil"
                    .into(),
            );
        }
    }
}

async fn run_portofolio_history_batch_scrape(
    session: Arc<Session>,
    codes: Vec<String>,
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

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!(
        "On-demand portofolio history batch: login → PIN → {} emiten...",
        codes.len()
    );

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let n = crate::portofolio_history_worker::scrape_and_upsert_portofolio_history_for_emitens(
            &page, &session, &ks, &codes,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok::<usize, String>(n)
    }
    .await;

    browser.close().await;

    result
}

/// On-demand scrape order/v2/list → upsert `pending_order`
/// (alur `pending_order_worker::scrape_and_insert_pending_order`).
/// Single-flight global; survive cancel RPC.
/// Returns jumlah baris yang di-upsert.
pub async fn scrape_pending_order_all(session: Arc<Session>) -> Result<usize, String> {
    let mut rx = {
        let mut slot = inflight_pending_order().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!(
                "On-demand pending_order: sudah berjalan — menunggu hasil (single-flight)..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            *slot = Some(rx.clone());
            let session = Arc::clone(&session);
            tokio::spawn(async move {
                let result = run_pending_order_all_scrape(session).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand pending_order selesai: {n} baris di-upsert."
                    ),
                    Err(e) => eprintln!("On-demand pending_order GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight_pending_order().lock().await = None;
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
            return Err("on-demand pending_order: channel ditutup sebelum ada hasil".into());
        }
    }
}

async fn run_pending_order_all_scrape(session: Arc<Session>) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape pending_order".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape pending_order".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();

    println!("On-demand pending_order: login → PIN → order/v2/list...");

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

/// GET IDX30 dari Stockbit → daftar symbol (UPPERCASE). Single-flight.
pub async fn fetch_idx30_symbols_from_stockbit() -> Result<Vec<String>, String> {
    fetch_index_symbols_from_stockbit(
        "IDX30",
        emiten_list_worker::IDX30_COMPANY_URL,
        inflight_idx30,
    )
    .await
}

/// GET LQ45 dari Stockbit → daftar symbol (UPPERCASE). Single-flight.
pub async fn fetch_lq45_symbols_from_stockbit() -> Result<Vec<String>, String> {
    fetch_index_symbols_from_stockbit(
        "LQ45",
        emiten_list_worker::LQ45_COMPANY_URL,
        inflight_lq45,
    )
    .await
}

/// GET IDX80 dari Stockbit → daftar symbol (UPPERCASE). Single-flight.
pub async fn fetch_idx80_symbols_from_stockbit() -> Result<Vec<String>, String> {
    fetch_index_symbols_from_stockbit(
        "IDX80",
        emiten_list_worker::IDX80_COMPANY_URL,
        inflight_idx80,
    )
    .await
}

/// GET Kompas100 dari Stockbit → daftar symbol (UPPERCASE). Single-flight.
pub async fn fetch_kompas100_symbols_from_stockbit() -> Result<Vec<String>, String> {
    fetch_index_symbols_from_stockbit(
        "Kompas100",
        emiten_list_worker::KOMPAS100_COMPANY_URL,
        inflight_kompas100,
    )
    .await
}

async fn fetch_index_symbols_from_stockbit(
    label: &'static str,
    url: &'static str,
    inflight: fn() -> &'static Mutex<Option<watch::Receiver<Option<Result<Vec<String>, String>>>>>,
) -> Result<Vec<String>, String> {
    let mut rx = {
        let mut slot = inflight().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!("On-demand {label}: sudah berjalan — menunggu hasil (single-flight)...");
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<Vec<String>, String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result = run_fetch_index_symbols(label, url).await;
                match &result {
                    Ok(syms) => println!("On-demand {label} selesai: {} symbol.", syms.len()),
                    Err(e) => eprintln!("On-demand {label} GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight().lock().await = None;
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
            return Err(format!("on-demand {label}: channel ditutup sebelum ada hasil"));
        }
    }
}

async fn run_fetch_index_symbols(label: &str, url: &str) -> Result<Vec<String>, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk on-demand scrape".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk on-demand scrape".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    println!("On-demand {label}: login + GET company list...");

    let (browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        emiten_list_worker::fetch_index_symbols(&page, url, label)
            .await
            .map_err(|e| e.to_string())
    }
    .await;

    browser.close().await;

    result
}

/// Buat order limit buy via DOM (mode trading + form). Single-flight.
/// `expiry_dom_value`: GFD=`"0"`, GTC=`"1"`.
pub async fn create_buy_limit_order(
    emiten_name: String,
    price: i32,
    lot: i32,
    expiry_dom_value: String,
) -> Result<(), String> {
    let mut rx = {
        let mut slot = inflight_buy_limit().lock().await;
        if let Some(existing) = slot.as_ref() {
            println!("On-demand buy_limit: sudah berjalan — menunggu hasil (single-flight)...");
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<(), String>>>(None);
            *slot = Some(rx.clone());
            tokio::spawn(async move {
                let result =
                    run_create_buy_limit_order(emiten_name, price, lot, expiry_dom_value).await;
                match &result {
                    Ok(()) => println!("On-demand buy_limit selesai: sukses."),
                    Err(e) => eprintln!("On-demand buy_limit GAGAL: {e}"),
                }
                let _ = tx.send(Some(result));
                *inflight_buy_limit().lock().await = None;
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
            return Err("on-demand buy_limit: channel ditutup sebelum ada hasil".into());
        }
    }
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

    let _browser_guard = browser_session_lock().lock().await;

    println!(
        "On-demand buy_limit: login → trading → form buy ({emiten_name} {price}x{lot})..."
    );

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
