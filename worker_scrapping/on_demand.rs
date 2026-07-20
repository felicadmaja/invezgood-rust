//! On-demand scrape Stockbit (movers, emiten_list, bandarmology).
//!
//! Scrape dijalankan di `tokio::spawn` + single-flight per `code_name`, supaya
//! cancel/timeout di sisi gRPC client **tidak** membatalkan scrape yang sedang jalan
//! (pola log: login OK → client retry berulang sebelum bearer warm-up selesai).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::Local;
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

static INFLIGHT_BANDAR_ALL_TIME: OnceLock<
    Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>>,
> = OnceLock::new();

fn inflight_map() -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_EMITEN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn inflight_stockbit_map(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<(), String>>>>> {
    INFLIGHT_EMITEN_STOCKBIT.get_or_init(|| Mutex::new(HashMap::new()))
}

fn inflight_bandar_all_time_map(
) -> &'static Mutex<HashMap<String, watch::Receiver<Option<Result<usize, String>>>>> {
    INFLIGHT_BANDAR_ALL_TIME.get_or_init(|| Mutex::new(HashMap::new()))
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
    code_name: &str,
) -> Result<(), String> {
    let code = code_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("code_name kosong".into());
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

    let (mut browser, page) = launch_page()
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

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

    result
}

/// Scrape Key Stats + Corp.Action + Profile dari Stockbit API untuk satu `code_name`
/// (tanpa cek Scylla / `update_at`); upsert `emiten_list`.
/// Bila `also_bandarmology`: setelah emiten_list, scrape API bandarmology untuk kode tersebut.
pub async fn scrape_emiten_list_from_stockbit_for_code(
    session: Arc<Session>,
    code_name: &str,
    also_bandarmology: bool,
) -> Result<(), String> {
    let code = code_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("code_name kosong".into());
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

    let (mut browser, page) = launch_page()
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

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

    result
}

/// On-demand scrape Top Gainer/Loser (movers) → upsert `emiten_trending` + seed `emiten_list`;
/// lalu baca `code_name` dari `emiten_list` (setelah seed) → keystats/profile/corpaction.
/// Tidak scrape bandarmology.
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

    let (mut browser, page) = launch_page()
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
        println!("On-demand: token-ring scan emiten_list.code_name (setelah seed movers)...");
        let existing = bandarmology_worker::fetch_emiten_list_code_names(session.as_ref(), &ks)
            .await
            .map_err(|e| e.to_string())?;
        let emitens = merge_codes_movers_first(&existing, &mover_codes);
        println!(
            "On-demand: {} emiten untuk key_stats/profile/corp (movers dulu={}, scan={}; tanpa bandarmology).",
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

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

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

/// Scrape bandarmology untuk satu emiten: bulan berjalan + historis max 36 bulan (3 tahun)
/// (aturan `bandarmology_worker::scrape_bandarmology_for_code_if_missing`).
/// Single-flight per kode; survive cancel RPC.
/// Returns jumlah baris bulan yang di-upsert.
pub async fn scrape_bandarmology_all_the_time_for_code(
    session: Arc<Session>,
    kode_emiten: &str,
) -> Result<usize, String> {
    let code = kode_emiten.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("kode_emiten kosong".into());
    }

    let mut rx = {
        let mut map = inflight_bandar_all_time_map().lock().await;
        if let Some(existing) = map.get(&code) {
            println!(
                "On-demand bandarmology all-time: {code} sudah berjalan — menunggu hasil..."
            );
            existing.clone()
        } else {
            let (tx, rx) = watch::channel::<Option<Result<usize, String>>>(None);
            map.insert(code.clone(), rx.clone());
            let session = Arc::clone(&session);
            let code_spawn = code.clone();
            tokio::spawn(async move {
                let result = run_bandarmology_all_the_time(session, &code_spawn).await;
                match &result {
                    Ok(n) => println!(
                        "On-demand bandarmology all-time selesai {code_spawn}: {n} bulan di-upsert."
                    ),
                    Err(e) => eprintln!(
                        "On-demand bandarmology all-time GAGAL {code_spawn}: {e}"
                    ),
                }
                let _ = tx.send(Some(result));
                inflight_bandar_all_time_map().lock().await.remove(&code_spawn);
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
                "on-demand bandarmology all-time {code}: channel ditutup sebelum ada hasil"
            ));
        }
    }
}

async fn run_bandarmology_all_the_time(
    session: Arc<Session>,
    code: &str,
) -> Result<usize, String> {
    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk scrape bandarmology".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk scrape bandarmology".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _browser_guard = browser_session_lock().lock().await;
    let ks = keyspace();
    let today = Local::now().date_naive();

    println!("On-demand bandarmology all-time: mulai untuk {code} (max 36 bulan)...");

    let (mut browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;

        let inserted = bandarmology_worker::scrape_bandarmology_for_code_if_missing(
            &page,
            session,
            &ks,
            today,
            code,
        )
        .await?;

        Ok::<usize, String>(inserted)
    }
    .await;

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

    result
}
