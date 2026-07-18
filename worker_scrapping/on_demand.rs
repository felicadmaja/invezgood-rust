//! On-demand scrape Stockbit untuk satu emiten (dipanggil dari gRPC `GetEmitenListByCodeName`).

use std::sync::{Arc, OnceLock};

use chrono::Local;
use scylla::client::session::Session;
use stockbit_browser::{
    dismiss_profile_avatar_modal, launch_page, open_stream_or_login,
};
use tokio::sync::Mutex;

use crate::{bandarmology_worker, emiten_list_worker};

static ON_DEMAND_SCRAPE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn scrape_lock() -> &'static Mutex<()> {
    ON_DEMAND_SCRAPE_LOCK.get_or_init(|| Mutex::new(()))
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}

/// Bila `emiten_list` belum ada: Key Stats + Corp.Action + Profile API → upsert `emiten_list`.
/// Lalu bila `bandarmology` agg hari ini (`YYYY-MM-DD_CODE`) belum ada: scrape bandarmology.
pub async fn ensure_emiten_data_for_code(
    session: Arc<Session>,
    code_name: &str,
) -> Result<(), String> {
    let code = code_name.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("code_name kosong".into());
    }

    let email = std::env::var("STOCKBIT_EMAIL")
        .map_err(|_| "STOCKBIT_EMAIL wajib diisi untuk on-demand scrape".to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD")
        .map_err(|_| "STOCKBIT_PASSWORD wajib diisi untuk on-demand scrape".to_string())?;
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD tidak boleh kosong".into());
    }

    let _guard = scrape_lock().lock().await;
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
        dismiss_profile_avatar_modal(&page)
            .await
            .map_err(|e| format!("dismiss modal: {e}"))?;

        emiten_list_worker::scrape_emiten_list_for_code(&page, session.as_ref(), &ks, &code)
            .await?;

        println!("On-demand: bandarmology API untuk {code}...");
        bandarmology_worker::scrape_bandarmology_for_code_if_missing(
            &page,
            session.as_ref(),
            &ks,
            today,
            &code,
        )
        .await?;

        Ok::<(), String>(())
    }
    .await;

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

    result?;
    println!("On-demand scrape selesai untuk {code}.");
    Ok(())
}

/// On-demand scrape Top Gainer/Loser (movers) → upsert `emiten_trending`.
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

    let _guard = scrape_lock().lock().await;
    let ks = keyspace();

    println!("On-demand: emiten_trending via market-mover API (Top Gainer/Loser)...");

    let (mut browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    let result = async {
        open_stream_or_login(&page, email.trim(), password.trim())
            .await
            .map_err(|e| format!("login Stockbit: {e}"))?;
        dismiss_profile_avatar_modal(&page)
            .await
            .map_err(|e| format!("dismiss modal: {e}"))?;

        crate::emiten_trending_worker::scrape_and_insert_movers(
            &page,
            session.as_ref(),
            &ks,
        )
        .await
        .map_err(|e| e.to_string())
    }
    .await;

    if let Err(e) = browser.close().await {
        eprintln!("Peringatan: gagal menutup browser: {e}");
    }

    result
}
