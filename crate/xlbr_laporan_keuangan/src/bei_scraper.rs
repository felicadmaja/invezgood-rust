//! Scrape inlineXBRL.zip dari idx.co.id via Chrome (pool Stockbit).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use scylla::client::session::Session;
use stockbit_browser::{acquire_browser_session, evaluate_resilient, launch_page, BrowserLockClass};
use tokio::time::sleep;

use crate::model::XlbrLaporanKeuanganRow;

const IDX_URL: &str = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan";
const IDX_HOME: &str = "https://www.idx.co.id/id/";
const YEAR_IDS: [&str; 5] = ["year4", "year3", "year2", "year1", "year0"];
const PERIOD_IDS: [&str; 4] = ["period0", "period1", "period2", "period3"];
const WAIT_TABLE_MS: u64 = 45_000;
const POLL_MS: u64 = 500;
const IDX_GOTO_MAX_ATTEMPTS: u32 = 6;

pub struct ScrapOutcome {
    pub uploaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub last_row: Option<XlbrLaporanKeuanganRow>,
}

pub async fn scrap_and_upload(
    db: Arc<Session>,
    code: &str,
) -> Result<ScrapOutcome, String> {
    let code = code.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("code wajib diisi".into());
    }

    let download_dir = download_dir();
    let screenshot_dir = screenshot_dir();
    tokio::fs::create_dir_all(&download_dir)
        .await
        .map_err(|e| format!("mkdir {}: {e}", download_dir.display()))?;
    tokio::fs::create_dir_all(&screenshot_dir)
        .await
        .map_err(|e| format!("mkdir {}: {e}", screenshot_dir.display()))?;

    cleanup_screenshot_dir(&screenshot_dir).await;
    cleanup_code_zips(&download_dir, &code).await;

    let _lock = acquire_browser_session(BrowserLockClass::Interactive)
        .await
        .map_err(|e| format!("Chrome lock: {e}"))?;
    let (_browser, page) = launch_page()
        .await
        .map_err(|e| format!("launch Chrome: {e}"))?;

    goto_idx(&page).await?;
    save_screenshot(&page, &screenshot_dir, &format!("{code}-01-open")).await;

    if page_idx_error(&page).await? {
        save_screenshot(&page, &screenshot_dir, &format!("{code}-01-error")).await;
        return Err(idx_unreachable_message().into());
    }
    if !page_idx_ready(&page).await? {
        save_screenshot(&page, &screenshot_dir, &format!("{code}-01-not-ready")).await;
        return Err(
            "halaman IDX belum siap (Search Company Code tidak muncul); coba lagi nanti".into(),
        );
    }

    search_company(&page, &code).await?;
    save_screenshot(&page, &screenshot_dir, &format!("{code}-02-selected")).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|e| e.to_string())?;

    let mut uploaded = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut last_row = None;
    let mut slot = 1usize;

    for year_id in YEAR_IDS {
        for period_id in PERIOD_IDS {
            let label = format!("{code}-{slot:02}-{year_id}-{period_id}");
            save_screenshot(&page, &screenshot_dir, &format!("{label}-before")).await;

            select_filters(&page, year_id, period_id).await?;
            click_terapkan(&page).await?;

            match wait_table(&page).await? {
                TableState::NotFound => {
                    skipped += 1;
                    save_screenshot(&page, &screenshot_dir, &format!("{label}-not-found")).await;
                    slot += 1;
                    continue;
                }
                TableState::Found => {}
            }

            save_screenshot(&page, &screenshot_dir, &format!("{label}-found")).await;

            let zip_path = download_dir.join(format!("{code}-{slot}.zip"));
            match download_inline_zip(&page, &client, &zip_path).await {
                Ok(()) => {
                    save_screenshot(&page, &screenshot_dir, &format!("{label}-downloaded")).await;
                    match upload_zip_file(db.clone(), &zip_path).await {
                        Ok(row) => {
                            uploaded += 1;
                            last_row = Some(row);
                            let _ = tokio::fs::remove_file(&zip_path).await;
                        }
                        Err(e) => {
                            failed += 1;
                            eprintln!("ScrapZipFromBei upload {zip_path:?} gagal: {e}");
                            save_screenshot(&page, &screenshot_dir, &format!("{label}-upload-fail"))
                                .await;
                        }
                    }
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("ScrapZipFromBei download {label} gagal: {e}");
                    save_screenshot(&page, &screenshot_dir, &format!("{label}-download-fail")).await;
                }
            }
            slot += 1;
        }
    }

    cleanup_code_zips(&download_dir, &code).await;
    save_screenshot(&page, &screenshot_dir, &format!("{code}-99-done")).await;

    Ok(ScrapOutcome {
        uploaded,
        skipped,
        failed,
        last_row,
    })
}

fn crate_src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn download_dir() -> PathBuf {
    crate_src_dir().join("downloaded_xbrl")
}

fn screenshot_dir() -> PathBuf {
    crate_src_dir().join("screenshot")
}

async fn cleanup_screenshot_dir(dir: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if meta.is_file() {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

async fn cleanup_code_zips(dir: &Path, code: &str) {
    let pattern = format!("{code}-");
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&pattern) && name.ends_with(".zip") {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

async fn goto_idx(page: &Page) -> Result<(), String> {
    let max_attempts = std::env::var("XLBR_IDX_GOTO_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(IDX_GOTO_MAX_ATTEMPTS);
    let base_wait = std::env::var("XLBR_IDX_RETRY_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8u64);

    for attempt in 1..=max_attempts {
        if attempt == 1 {
            let _ = page.goto(IDX_HOME).await;
            sleep(Duration::from_secs(2)).await;
        }

        page.goto(IDX_URL)
            .await
            .map_err(|e| format!("goto IDX (attempt {attempt}): {e}"))?;

        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(20) {
            if page_idx_ready(page).await? {
                eprintln!("ScrapZipFromBei: halaman IDX siap (attempt {attempt})");
                return Ok(());
            }
            if page_idx_error(page).await? {
                break;
            }
            sleep(Duration::from_millis(POLL_MS)).await;
        }

        let wait_secs = base_wait.saturating_mul(attempt as u64);
        eprintln!(
            "ScrapZipFromBei: IDX error/belum siap (attempt {attempt}/{max_attempts}) — \
             retry dalam {wait_secs}s"
        );
        if attempt < max_attempts {
            sleep(Duration::from_secs(wait_secs)).await;
            let _ = page.reload().await;
            sleep(Duration::from_secs(2)).await;
        }
    }

    Err(idx_unreachable_message())
}

fn idx_unreachable_message() -> String {
    "IDX www.idx.co.id tidak dapat diakses (503 Varnish / 403 blocked / backend down). \
     Server BEI sedang overload atau memblokir akses bot — coba lagi beberapa menit kemudian \
     atau akses manual dari browser desktop di jaringan yang sama."
        .into()
}

async fn page_idx_error(page: &Page) -> Result<bool, String> {
    let text = eval_string(
        page,
        r#"(() => (document.body && document.body.innerText) ? document.body.innerText : '')"#,
    )
    .await?;
    let lower = text.to_ascii_lowercase();
    Ok(lower.contains("error 503")
        || lower.contains("error 403")
        || lower.contains("backend fetch failed")
        || lower.contains("guru meditation")
        || lower.contains("varnish cache server")
        || lower.contains("cloudflare")
        || lower.contains("access denied")
        || lower.contains("attention required"))
}

async fn page_idx_ready(page: &Page) -> Result<bool, String> {
    eval_bool(
        page,
        r#"(() => !!document.querySelector('input[placeholder="Search Company Code"]'))()"#,
    )
    .await
}

async fn search_company(page: &Page, code: &str) -> Result<(), String> {
    let code_js = js_str(code);
    let filled = eval_bool(
        page,
        &format!(
            r#"(() => {{
  const input = document.querySelector('input[placeholder="Search Company Code"]');
  if (!input) return false;
  input.focus();
  input.value = {code_js};
  input.dispatchEvent(new Event('input', {{ bubbles: true }}));
  input.dispatchEvent(new Event('change', {{ bubbles: true }}));
  return true;
}})()"#
        ),
    )
    .await?;
    if !filled {
        return Err("input Search Company Code tidak ditemukan".into());
    }

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(20) {
        if eval_bool(
            page,
            "(() => !!document.querySelector('#vs3__listbox'))()",
        )
        .await?
        {
            break;
        }
        sleep(Duration::from_millis(POLL_MS)).await;
    }

    let clicked = eval_bool(
        page,
        &format!(
            r#"(() => {{
  const code = {code_js};
  const items = [...document.querySelectorAll('#vs3__listbox li')];
  const hit = items.find(li => li.textContent && li.textContent.toUpperCase().includes(code));
  if (!hit) return false;
  hit.click();
  return true;
}})()"#
        ),
    )
    .await?;
    if !clicked {
        return Err(format!("emiten {code} tidak ada di dropdown IDX"));
    }
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn select_filters(page: &Page, year_id: &str, period_id: &str) -> Result<(), String> {
    click_by_id(page, year_id).await?;
    click_by_id(page, period_id).await?;
    Ok(())
}

async fn click_terapkan(page: &Page) -> Result<(), String> {
    let clicked = eval_bool(
        page,
        r#"(() => {
  const buttons = [...document.querySelectorAll('button')];
  const btn = buttons.find(b => b.textContent && b.textContent.trim() === 'Terapkan');
  if (!btn) return false;
  btn.click();
  return true;
})()"#,
    )
    .await?;
    if !clicked {
        return Err("tombol Terapkan tidak ditemukan".into());
    }
    sleep(Duration::from_millis(800)).await;
    Ok(())
}

enum TableState {
    Found,
    NotFound,
}

async fn wait_table(page: &Page) -> Result<TableState, String> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(WAIT_TABLE_MS) {
        let state = eval_string(
            page,
            r#"(() => {
  const text = document.body ? document.body.innerText : '';
  if (text.includes('Data tidak ditemukan')) return 'not_found';
  const tds = [...document.querySelectorAll('td')];
  if (tds.some(td => td.textContent && td.textContent.trim() === 'inlineXBRL.zip')) return 'found';
  return 'waiting';
})()"#,
        )
        .await?;
        match state.as_str() {
            "found" => return Ok(TableState::Found),
            "not_found" => return Ok(TableState::NotFound),
            _ => sleep(Duration::from_millis(POLL_MS)).await,
        }
    }
    Err("timeout menunggu tabel laporan IDX".into())
}

async fn download_inline_zip(page: &Page, client: &reqwest::Client, dest: &Path) -> Result<(), String> {
    let href = eval_string(
        page,
        r#"(() => {
  const tds = [...document.querySelectorAll('td')];
  const zipTd = tds.find(td => td.textContent && td.textContent.trim() === 'inlineXBRL.zip');
  if (!zipTd) return '';
  const tr = zipTd.closest('tr');
  if (!tr) return '';
  const links = [...tr.querySelectorAll('a[href]')];
  for (const a of links) {
    const h = a.getAttribute('href');
    if (h && h !== '#' && !h.startsWith('javascript:')) return h;
  }
  return '';
})()"#,
    )
    .await?;
    if href.is_empty() {
        return Err("link download inlineXBRL.zip tidak ditemukan".into());
    }

    let url = resolve_url(page, &href).await?;
    let bytes = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?
        .error_for_status()
        .map_err(|e| format!("GET {url} status: {e}"))?
        .bytes()
        .await
        .map_err(|e| format!("baca body {url}: {e}"))?;

    if bytes.len() < 4 || bytes[0] != b'P' || bytes[1] != b'K' {
        return Err(format!("unduhan bukan zip valid dari {url}"));
    }

    tokio::fs::write(dest, &bytes)
        .await
        .map_err(|e| format!("tulis {}: {e}", dest.display()))?;
    Ok(())
}

async fn resolve_url(page: &Page, href: &str) -> Result<String, String> {
    if href.starts_with("http://") || href.starts_with("https://") {
        return Ok(href.to_string());
    }
    let base = page
        .url()
        .await
        .map_err(|e| format!("url page: {e}"))?
        .unwrap_or_else(|| IDX_URL.to_string());
    if href.starts_with('/') {
        let origin = base
            .split('/')
            .take(3)
            .collect::<Vec<_>>()
            .join("/");
        return Ok(format!("{origin}{href}"));
    }
    let prefix = base.rsplit_once('/').map(|(p, _)| p).unwrap_or(&base);
    Ok(format!("{prefix}/{href}"))
}

async fn upload_zip_file(
    db: Arc<Session>,
    path: &Path,
) -> Result<XlbrLaporanKeuanganRow, String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| format!("baca {}: {e}", path.display()))?;
    crate::upload_from_zip_bytes(db, &bytes).await
}

async fn click_by_id(page: &Page, id: &str) -> Result<(), String> {
    let id_js = js_str(id);
    let ok = eval_bool(
        page,
        &format!(
            r#"(() => {{
  const el = document.getElementById({id_js});
  if (!el) return false;
  el.click();
  return true;
}})()"#
        ),
    )
    .await?;
    if !ok {
        return Err(format!("element #{id} tidak ditemukan"));
    }
    sleep(Duration::from_millis(200)).await;
    Ok(())
}

async fn save_screenshot(page: &Page, dir: &Path, name: &str) -> Option<PathBuf> {
    let path = dir.join(format!("{name}.png"));
    match page
        .save_screenshot(
            ScreenshotParams::builder()
                .format(CaptureScreenshotFormat::Png)
                .full_page(true)
                .build(),
            &path,
        )
        .await
    {
        Ok(_) => {
            eprintln!("ScrapZipFromBei screenshot: {}", path.display());
            Some(path)
        }
        Err(e) => {
            eprintln!("ScrapZipFromBei screenshot gagal [{name}]: {e}");
            None
        }
    }
}

fn js_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
}

async fn eval_bool(page: &Page, js: &str) -> Result<bool, String> {
    let v = evaluate_resilient(page, js)
        .await
        .map_err(|e| e.to_string())?;
    Ok(v.value().and_then(|x| x.as_bool()).unwrap_or(false))
}

async fn eval_string(page: &Page, js: &str) -> Result<String, String> {
    let v = evaluate_resilient(page, js)
        .await
        .map_err(|e| e.to_string())?;
    Ok(v.value()
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string())
}
