//! Chrome pool terpisah untuk scrape idx.co.id (BEI) — tidak memakai profil / lock Stockbit.

use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::network::ClearBrowserCookiesParams;
use chromiumoxide::cdp::browser_protocol::storage::ClearDataForOriginParams;
use chromiumoxide::page::Page;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{sleep, timeout};

use crate::{
    apply_desktop_viewport, browser_config_for_dir, clear_stale_chrome_locks,
    terminate_stale_chrome_processes, workspace_root, StockbitError,
};

const IDX_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

static IDX_BROWSER_SESSION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const IDX_LOCK_TIMEOUT: Duration = Duration::from_secs(45);

struct PersistentIdxChrome {
    browser: Browser,
    page: Page,
}

static PERSISTENT_IDX_CHROME: OnceLock<Mutex<Option<PersistentIdxChrome>>> = OnceLock::new();

fn idx_persistent_chrome() -> &'static Mutex<Option<PersistentIdxChrome>> {
    PERSISTENT_IDX_CHROME.get_or_init(|| Mutex::new(None))
}

fn idx_browser_session_lock() -> &'static Mutex<()> {
    IDX_BROWSER_SESSION_LOCK.get_or_init(|| Mutex::new(()))
}

/// Direktori profil Chrome khusus BEI/IDX (terpisah dari Stockbit).
pub fn idx_browser_data_dir() -> PathBuf {
    std::env::var("BEI_BROWSER_DATA_DIR")
        .or_else(|_| std::env::var("IDX_BROWSER_DATA_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            workspace_root()
                .join("crate")
                .join("xlbr_laporan_keuangan")
                .join("browser_data")
        })
}

/// Lock exclusive Chrome BEI — independen dari mutex Stockbit.
pub async fn acquire_idx_browser_session() -> Result<MutexGuard<'static, ()>, String> {
    match timeout(IDX_LOCK_TIMEOUT, idx_browser_session_lock().lock()).await {
        Ok(guard) => Ok(guard),
        Err(_) => Err(
            "Chrome BEI sibuk (scrap idx.co.id masih berjalan). Coba lagi sebentar.".into(),
        ),
    }
}

/// Handle BEI — `close()` tidak mematikan Chrome (pool persistent terpisah).
pub struct IdxBrowserSession;

impl IdxBrowserSession {
    pub async fn close(self) {
        // no-op: BEI Chrome tetap hidup di pool sendiri
    }
}

async fn idx_page_is_alive(page: &Page) -> bool {
    match timeout(Duration::from_secs(5), page.evaluate("1+1")).await {
        Ok(Ok(eval)) => eval.into_value::<i64>().ok() == Some(2),
        _ => false,
    }
}

async fn launch_fresh_idx_browser() -> Result<(Browser, Page), StockbitError> {
    let data_dir = idx_browser_data_dir();
    let config = browser_config_for_dir(&data_dir, true)?;
    let (browser, mut handler) = Browser::launch(config).await?;
    tokio::task::spawn(async move { while handler.next().await.is_some() {} });

    let page = browser.new_page("about:blank").await?;
    page.set_user_agent(IDX_USER_AGENT).await?;
    apply_desktop_viewport(&page).await?;
    page.evaluate_on_new_document(
        r#"
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    "#,
    )
    .await?;

    Ok((browser, page))
}

/// Ambil page Chrome BEI (pool terpisah dari Stockbit).
pub async fn launch_idx_page() -> Result<(IdxBrowserSession, Page), StockbitError> {
    let mut slot = idx_persistent_chrome().lock().await;

    if let Some(existing) = slot.as_ref() {
        if idx_page_is_alive(&existing.page).await {
            eprintln!("Chrome BEI: reuse pool idx.co.id (profil terpisah dari Stockbit)");
            apply_desktop_viewport(&existing.page).await?;
            return Ok((IdxBrowserSession, existing.page.clone()));
        }
        eprintln!("Chrome BEI: page tidak responsif — relaunch...");
        if let Some(mut old) = slot.take() {
            let _ = old.browser.close().await;
        }
        sleep(Duration::from_millis(500)).await;
    } else {
        eprintln!("Chrome BEI: launch baru (profil {})...", idx_browser_data_dir().display());
    }

    let (browser, page) = launch_fresh_idx_browser().await?;
    eprintln!("Chrome BEI: ready");
    *slot = Some(PersistentIdxChrome {
        browser,
        page: page.clone(),
    });
    Ok((IdxBrowserSession, page))
}

/// Hapus cookie & storage idx.co.id — reset sesi BEI dari awal setiap scrape.
pub async fn reset_idx_browser_state(page: &Page) -> Result<(), StockbitError> {
    page.execute(ClearBrowserCookiesParams::default()).await?;
    for origin in ["https://www.idx.co.id", "https://idx.co.id"] {
        let params = ClearDataForOriginParams::builder()
            .origin(origin)
            .storage_types("cookies,local_storage,session_storage,indexeddb,cache_storage")
            .build()
            .map_err(|e| format!("ClearDataForOrigin build ({origin}): {e}"))?;
        if let Err(e) = page.execute(params).await {
            eprintln!("Chrome BEI: clear storage {origin} — {e}");
        }
    }
    let _ = page.goto("about:blank").await;
    sleep(Duration::from_millis(300)).await;
    eprintln!("Chrome BEI: cookie & storage idx.co.id dihapus — sesi fresh");
    Ok(())
}

/// Paksa tutup Chrome BEI (opsional maintenance).
pub async fn shutdown_idx_browser() -> Result<(), StockbitError> {
    let mut slot = idx_persistent_chrome().lock().await;
    if let Some(mut old) = slot.take() {
        eprintln!("Chrome BEI: shutdown...");
        let _ = old.browser.close().await;
        sleep(Duration::from_millis(500)).await;
        let data_dir = idx_browser_data_dir();
        terminate_stale_chrome_processes(&data_dir);
        clear_stale_chrome_locks(&data_dir);
    }
    Ok(())
}
