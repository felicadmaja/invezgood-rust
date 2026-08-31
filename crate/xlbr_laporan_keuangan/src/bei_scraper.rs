//! Scrape inlineXBRL.zip dari idx.co.id via Chrome BEI (profil terpisah dari Stockbit).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use scylla::client::session::Session;
use stockbit_browser::{
    acquire_idx_browser_session, apply_desktop_viewport, evaluate_resilient, launch_idx_page,
};
use tokio::sync::{Mutex, watch};
use tokio::time::{sleep, timeout};

use crate::model::XlbrLaporanKeuanganRow;

static SCRAP_JOB_GENERATION: AtomicU64 = AtomicU64::new(0);

struct InflightScrap {
    code: String,
    generation: u64,
    status: watch::Sender<ScrapJobStatus>,
}

#[derive(Clone)]
pub enum ScrapJobStatus {
    Running,
    Finished(Result<ScrapOutcome, String>),
}

static SCRAP_INFLIGHT: OnceLock<Mutex<Option<InflightScrap>>> = OnceLock::new();

fn scrap_inflight() -> &'static Mutex<Option<InflightScrap>> {
    SCRAP_INFLIGHT.get_or_init(|| Mutex::new(None))
}

pub enum ScrapStart {
    Started { job_gen: u64, watch: watch::Receiver<ScrapJobStatus> },
}

async fn cancel_previous_scrap_and_wait() {
    let prev_code = {
        let slot = scrap_inflight().lock().await;
        slot.as_ref().map(|s| s.code.clone())
    };
    let Some(prev_code) = prev_code else {
        return;
    };

    SCRAP_JOB_GENERATION.fetch_add(1, Ordering::SeqCst);
    eprintln!("ScrapZipFromBei: batalkan scrap {prev_code} sebelumnya — tunggu BEI Chrome lock lepas");

    let deadline = Instant::now() + Duration::from_millis(SCRAP_CANCEL_WAIT_MS);
    while Instant::now() < deadline {
        if scrap_inflight().lock().await.is_none() {
            eprintln!("ScrapZipFromBei: BEI Chrome lock scrap sebelumnya lepas");
            sleep(Duration::from_millis(400)).await;
            return;
        }
        sleep(Duration::from_millis(200)).await;
    }

    eprintln!(
        "ScrapZipFromBei: WARNING scrap sebelumnya belum selesai setelah {SCRAP_CANCEL_WAIT_MS}ms — lanjut acquire lock"
    );
}

/// Invoke baru: batalkan scrap/Chrome lock sebelumnya (mis. client logout), lalu mulai scrap fresh.
pub async fn try_begin_scrap_job(code: &str) -> ScrapStart {
    cancel_previous_scrap_and_wait().await;

    let code = code.trim().to_ascii_uppercase();
    let mut slot = scrap_inflight().lock().await;
    let generation = SCRAP_JOB_GENERATION.fetch_add(1, Ordering::SeqCst) + 1;
    let (status_tx, status_rx) = watch::channel(ScrapJobStatus::Running);
    *slot = Some(InflightScrap {
        code,
        generation,
        status: status_tx,
    });
    ScrapStart::Started {
        job_gen: generation,
        watch: status_rx,
    }
}

pub async fn wait_scrap_outcome(
    mut watch: watch::Receiver<ScrapJobStatus>,
) -> Result<ScrapOutcome, String> {
    loop {
        if let ScrapJobStatus::Finished(result) = watch.borrow().clone() {
            return result;
        }
        watch
            .changed()
            .await
            .map_err(|_| "scrap task terminated unexpectedly".to_string())?;
    }
}

pub async fn finish_scrap_job(generation: u64, result: Result<ScrapOutcome, String>) {
    let mut slot = scrap_inflight().lock().await;
    if let Some(active) = slot.as_ref() {
        if active.generation == generation {
            let _ = active
                .status
                .send(ScrapJobStatus::Finished(result.clone()));
        }
    }
    if slot.as_ref().is_some_and(|s| s.generation == generation) {
        *slot = None;
    }
}

fn scrap_job_cancelled(job_gen: u64) -> bool {
    job_gen != SCRAP_JOB_GENERATION.load(Ordering::SeqCst)
}

fn scrap_cancelled_err() -> String {
    "scrap dibatalkan (invoke ScrapZipFromBei baru)".into()
}

fn check_scrap_job(job_gen: u64) -> Result<(), String> {
    if scrap_job_cancelled(job_gen) {
        Err(scrap_cancelled_err())
    } else {
        Ok(())
    }
}

const IDX_URL: &str = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan";
const YEAR_IDS: [&str; 5] = ["year4", "year3", "year2", "year1", "year0"];
const PERIOD_IDS: [&str; 4] = ["period0", "period1", "period2", "period3"];
const SCRAP_CANCEL_WAIT_MS: u64 = 15_000;
const WAIT_TABLE_MS: u64 = 25_000;
const SEARCH_DROPDOWN_WAIT_MS: u64 = 45_000;
const SEARCH_EVAL_TIMEOUT_MS: u64 = 15_000;
const SEARCH_TYPE_CHAR_MS: u64 = 100;
const SEARCH_POLL_MS: u64 = 250;
const POLL_MS: u64 = 400;
const IDX_GOTO_MAX_ATTEMPTS: u32 = 6;

#[derive(Clone)]
pub struct ScrapOutcome {
    pub uploaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub last_row: Option<XlbrLaporanKeuanganRow>,
}

pub async fn scrap_and_upload(
    db: Arc<Session>,
    code: &str,
    job_gen: u64,
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
    check_scrap_job(job_gen)?;

    let _lock = acquire_idx_browser_session()
        .await
        .map_err(|e| format!("Chrome BEI lock: {e}"))?;
    let (_browser, page) = launch_idx_page()
        .await
        .map_err(|e| format!("launch Chrome BEI: {e}"))?;
    apply_desktop_viewport(&page)
        .await
        .map_err(|e| format!("viewport desktop: {e}"))?;

    goto_idx(&page, job_gen).await?;
    apply_desktop_viewport(&page)
        .await
        .map_err(|e| format!("viewport desktop setelah goto IDX: {e}"))?;
    save_screenshot(&page, &screenshot_dir, &format!("{code}-01-open")).await;
    check_scrap_job(job_gen)?;

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

    search_company(&page, &code, job_gen).await?;
    apply_desktop_viewport(&page)
        .await
        .map_err(|e| format!("viewport desktop setelah pilih emiten: {e}"))?;
    save_screenshot(&page, &screenshot_dir, &format!("{code}-02-selected")).await;

    let mut uploaded = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut last_row = None;
    let mut slot = 1usize;

    for year_id in YEAR_IDS {
        for period_id in PERIOD_IDS {
            check_scrap_job(job_gen)?;
            let label = format!("{code}-{slot:02}-{year_id}-{period_id}");
            save_screenshot(&page, &screenshot_dir, &format!("{label}-before")).await;

            reset_filters(&page).await?;
            select_filters(&page, year_id, period_id).await?;
            click_terapkan(&page).await?;
            apply_desktop_viewport(&page)
                .await
                .map_err(|e| format!("viewport desktop setelah Terapkan: {e}"))?;

            match wait_table(&page, job_gen).await? {
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
            match download_inline_zip(&page, &zip_path).await {
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

async fn goto_idx(page: &Page, job_gen: u64) -> Result<(), String> {
    let max_attempts = std::env::var("XLBR_IDX_GOTO_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(IDX_GOTO_MAX_ATTEMPTS);
    let base_wait = std::env::var("XLBR_IDX_RETRY_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8u64);

    for attempt in 1..=max_attempts {
        check_scrap_job(job_gen)?;

        page.goto(IDX_URL)
            .await
            .map_err(|e| format!("goto IDX (attempt {attempt}): {e}"))?;

        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(20) {
            check_scrap_job(job_gen)?;
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
            check_scrap_job(job_gen)?;
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

async fn search_company(page: &Page, code: &str, job_gen: u64) -> Result<(), String> {
    check_scrap_job(job_gen)?;
    eprintln!("ScrapZipFromBei: cari emiten {code} di dropdown IDX");

    let opened = eval_bool(
        page,
        r#"(() => {
  for (const btn of document.querySelectorAll('#vs3 .vs__clear, #vs3 .vs__deselect, .vs__clear')) {
    btn.click();
  }
  const toggle = document.querySelector('#vs3 .vs__dropdown-toggle')
    || document.querySelector('#vs3__combobox');
  if (toggle) toggle.click();
  const input = document.querySelector('#vs3 input.vs__search')
    || document.querySelector('input[placeholder="Search Company Code"]');
  return !!input;
})()"#,
    )
    .await?;
    if !opened {
        return Err("input Search Company Code (#vs3) tidak ditemukan".into());
    }
    sleep(Duration::from_millis(250)).await;
    check_scrap_job(job_gen)?;

    let input = page
        .find_element("#vs3 input.vs__search, input[placeholder=\"Search Company Code\"]")
        .await
        .map_err(|_| "elemen input dropdown emiten tidak ditemukan".to_string())?;
    input
        .click()
        .await
        .map_err(|e| format!("klik input emiten: {e}"))?;
    sleep(Duration::from_millis(150)).await;

    // Fokus input; ketik per huruf via keyboard CDP (vue-select IDX tidak merespons setter JS).
    let _ = page
        .evaluate(
            r#"(() => {
  const input = document.querySelector('#vs3 input.vs__search')
    || document.querySelector('input[placeholder="Search Company Code"]');
  if (!input) return false;
  input.focus();
  if (typeof input.select === 'function') input.select();
  return true;
})()"#,
        )
        .await;

    eprintln!("ScrapZipFromBei: ketik kode emiten {code} (tunggu #vs3__listbox)");
    for ch in code.chars() {
        check_scrap_job(job_gen)?;
        input
            .type_str(&ch.to_string())
            .await
            .map_err(|e| format!("ketik emiten {code}: {e}"))?;
        sleep(Duration::from_millis(SEARCH_TYPE_CHAR_MS)).await;
    }

    let code_js = js_str(code);
    let started = Instant::now();
    let mut last_hint = String::from("menunggu listbox");
    while started.elapsed() < Duration::from_millis(SEARCH_DROPDOWN_WAIT_MS) {
        check_scrap_job(job_gen)?;
        let eval = timeout(
            Duration::from_millis(SEARCH_EVAL_TIMEOUT_MS),
            page.evaluate(format!(
                r#"(() => {{
  const code = {code_js};
  const listbox = document.querySelector('#vs3__listbox');
  if (!listbox) return {{ clicked: false, step: 'no_listbox', count: 0 }};
  const items = [...listbox.querySelectorAll('li[role="option"], li.vs__dropdown-option')];
  if (items.length === 0) return {{ clicked: false, step: 'listbox_empty', count: 0 }};
  for (const li of items) {{
    const strong = li.querySelector('strong');
    const ticker = (strong ? strong.textContent : li.textContent || '').trim().toUpperCase();
    const m = ticker.match(/^([A-Z0-9]{{1,5}})/);
    const tok = m ? m[1] : ticker.split(/\s+/)[0];
    if (tok === code) {{
      li.click();
      return {{ clicked: true, step: 'clicked', count: items.length, ticker: tok }};
    }}
  }}
  return {{ clicked: false, step: 'no_exact_match', count: items.length }};
}})()"#
            )),
        )
        .await
        .map_err(|_| format!("timeout eval listbox emiten {code}"))?
        .map_err(|e| format!("evaluate listbox emiten: {e}"))?;

        let val = eval
            .value()
            .ok_or_else(|| format!("listbox {code}: tidak ada return value"))?;
        let clicked = val.get("clicked").and_then(|x| x.as_bool()).unwrap_or(false);
        if clicked {
            sleep(Duration::from_millis(400)).await;
            let got = eval_string(
                page,
                r#"(() => {
  const sel = document.querySelector('#vs3 .vs__selected-options, #vs3 .vs__selected');
  if (!sel) return '';
  const strong = sel.querySelector('strong');
  const text = (strong ? strong.textContent : sel.textContent || '').trim().toUpperCase();
  const m = text.match(/^([A-Z0-9]{1,5})/);
  return m ? m[1] : text.split(/\s+/)[0];
})()"#,
            )
            .await?;
            if got == code {
                eprintln!("ScrapZipFromBei: emiten {code} terpilih");
                sleep(Duration::from_millis(300)).await;
                return Ok(());
            }
            last_hint = format!("klik OK tapi terpilih={got}");
        } else {
            let step = val
                .get("step")
                .and_then(|x| x.as_str())
                .unwrap_or("?");
            let count = val.get("count").and_then(|x| x.as_u64()).unwrap_or(0);
            last_hint = format!("{step} (li={count})");
        }
        sleep(Duration::from_millis(SEARCH_POLL_MS)).await;
    }

    save_screenshot_stuck(page).await;
    Err(format!(
        "timeout dropdown emiten {code} setelah {}ms — terakhir: {last_hint}",
        started.elapsed().as_millis()
    ))
}

async fn reset_filters(page: &Page) -> Result<(), String> {
    let clicked = eval_bool(
        page,
        r#"(() => {
  const buttons = [...document.querySelectorAll('button')];
  const btn = buttons.find(b => b.textContent && b.textContent.trim() === 'RESET');
  if (!btn) return false;
  btn.click();
  return true;
})()"#,
    )
    .await?;
    if clicked {
        sleep(Duration::from_millis(300)).await;
    }
    Ok(())
}

async fn select_filters(page: &Page, year_id: &str, period_id: &str) -> Result<(), String> {
    let year_js = js_str(year_id);
    let period_js = js_str(period_id);
    let ok = eval_bool(
        page,
        &format!(
            r#"(() => {{
  function clickFilter(id) {{
    const el = document.getElementById(id);
    if (!el) return false;
    const label = document.querySelector('label[for="' + id + '"]');
    if (label) label.click();
    el.click();
    if (el.tagName === 'INPUT') {{
      el.checked = true;
      el.dispatchEvent(new Event('change', {{ bubbles: true }}));
    }}
    return true;
  }}
  return clickFilter({year_js}) && clickFilter({period_js});
}})()"#
        ),
    )
    .await?;
    if !ok {
        return Err(format!(
            "filter tahun/periode tidak ditemukan ({year_id}, {period_id})"
        ));
    }
    sleep(Duration::from_millis(300)).await;
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
    sleep(Duration::from_millis(500)).await;
    Ok(())
}

const TABLE_POLL_JS: &str = r#"(() => {
  const text = document.body ? document.body.innerText : '';
  if (text.includes('Data tidak ditemukan')) return 'not_found';
  const tds = [...document.querySelectorAll('td')];
  if (tds.some(td => td.textContent && td.textContent.trim() === 'inlineXBRL.zip')) return 'found';

  const spinnerSel = '[class*="spinner"], [class*="Spinner"], [class*="loading"], [class*="Loading"], [role="progressbar"]';
  for (const el of document.querySelectorAll(spinnerSel)) {
    const r = el.getBoundingClientRect();
    if (r.width > 8 && r.height > 8 && r.bottom > 0 && r.top < window.innerHeight) {
      return 'loading';
    }
  }

  if (/\d+\s+dari\s+\d+/i.test(text) && tds.length < 4 && !text.includes('Data tidak ditemukan')) {
    return 'loading';
  }

  return 'waiting';
})()"#;

async fn table_poll_state(page: &Page) -> Result<String, String> {
    eval_string(page, TABLE_POLL_JS).await
}

enum TableState {
    Found,
    NotFound,
}

async fn wait_table(page: &Page, job_gen: u64) -> Result<TableState, String> {
    let started = Instant::now();
    let mut retried_terapkan = false;
    let mut last_state = String::from("waiting");
    while started.elapsed() < Duration::from_millis(WAIT_TABLE_MS) {
        check_scrap_job(job_gen)?;
        let state = table_poll_state(page).await?;
        last_state = state.clone();
        match state.as_str() {
            "found" => return Ok(TableState::Found),
            "not_found" => return Ok(TableState::NotFound),
            "loading" => {
                if !retried_terapkan && started.elapsed() > Duration::from_secs(12) {
                    eprintln!("ScrapZipFromBei: loading lama — klik Terapkan ulang");
                    let _ = click_terapkan(page).await;
                    retried_terapkan = true;
                }
                sleep(Duration::from_millis(POLL_MS)).await;
            }
            _ => sleep(Duration::from_millis(POLL_MS)).await,
        }
    }
    save_screenshot_stuck(page).await;
    eprintln!(
        "ScrapZipFromBei: timeout tabel (state={last_state}) — anggap tidak ada data, lanjut slot berikutnya"
    );
    Ok(TableState::NotFound)
}

async fn save_screenshot_stuck(page: &Page) {
    let dir = screenshot_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
    let path = dir.join("STUCK-table-timeout.png");
    let _ = page
        .save_screenshot(
            ScreenshotParams::builder()
                .format(CaptureScreenshotFormat::Png)
                .full_page(false)
                .build(),
            &path,
        )
        .await;
    eprintln!("ScrapZipFromBei screenshot: {}", path.display());
}

async fn download_inline_zip(page: &Page, dest: &Path) -> Result<(), String> {
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
    let bytes = match download_via_browser_fetch(page, &url).await {
        Ok(bytes) => bytes,
        Err(browser_err) => {
            eprintln!(
                "ScrapZipFromBei: browser fetch gagal ({browser_err}) — coba reqwest+cookie"
            );
            download_via_reqwest_cookies(page, &url).await?
        }
    };

    write_zip_bytes(dest, &url, &bytes).await
}

async fn download_via_browser_fetch(page: &Page, url: &str) -> Result<Vec<u8>, String> {
    let url_js = js_str(url);
    let eval = page
        .evaluate_function(format!(
            r#"async () => {{
  const url = {url_js};
  const resp = await fetch(url, {{ credentials: 'include' }});
  if (!resp.ok) {{
    return {{ ok: false, status: resp.status, b64: '', len: 0 }};
  }}
  const buf = await resp.arrayBuffer();
  const bytes = new Uint8Array(buf);
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {{
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }}
  return {{ ok: true, status: resp.status, b64: btoa(bin), len: bytes.length }};
}}"#,
        ))
        .await
        .map_err(|e| format!("browser fetch evaluate: {e}"))?;

    let val = eval
        .value()
        .ok_or_else(|| "browser fetch: tidak ada return value".to_string())?;
    let ok = val.get("ok").and_then(|x| x.as_bool()).unwrap_or(false);
    let status = val.get("status").and_then(|x| x.as_u64()).unwrap_or(0);
    if !ok {
        return Err(format!("browser fetch {url} HTTP {status}"));
    }
    let b64 = val
        .get("b64")
        .and_then(|x| x.as_str())
        .unwrap_or_default();
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
        .map_err(|e| format!("browser fetch decode base64: {e}"))
}

async fn download_via_reqwest_cookies(page: &Page, url: &str) -> Result<Vec<u8>, String> {
    let cookie_header = browser_cookie_header(page).await?;
    let referer = page
        .url()
        .await
        .map_err(|e| format!("url page: {e}"))?
        .unwrap_or_else(|| IDX_URL.to_string());
    let user_agent = page
        .user_agent()
        .await
        .unwrap_or_else(|_| DEFAULT_DOWNLOAD_USER_AGENT.to_string());

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|e| e.to_string())?;

    let encoded_url = encode_download_url(url);
    let mut req = client
        .get(&encoded_url)
        .header("Accept", "application/zip,application/octet-stream,*/*")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Referer", referer)
        .header("User-Agent", user_agent);
    if !cookie_header.is_empty() {
        req = req.header("Cookie", cookie_header);
    }

    req.send()
        .await
        .map_err(|e| format!("GET {encoded_url}: {e}"))?
        .error_for_status()
        .map_err(|e| format!("GET {encoded_url} status: {e}"))?
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| format!("baca body {encoded_url}: {e}"))
}

async fn browser_cookie_header(page: &Page) -> Result<String, String> {
    let cookies = page
        .get_cookies()
        .await
        .map_err(|e| format!("get_cookies: {e}"))?;
    Ok(cookies
        .iter()
        .map(|c| format!("{}={}", c.name, c.value))
        .collect::<Vec<_>>()
        .join("; "))
}

fn encode_download_url(url: &str) -> String {
    let Some((base, path)) = url.split_once("://") else {
        return url.replace(' ', "%20");
    };
    let Some((authority, path)) = path.split_once('/') else {
        return url.replace(' ', "%20");
    };
    let encoded_path = path
        .split('/')
        .map(|seg| {
            seg.chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || "-._~".contains(ch) {
                        ch.to_string()
                    } else {
                        format!("%{:02X}", ch as u8)
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/");
    format!("{base}://{authority}/{encoded_path}")
}

const DEFAULT_DOWNLOAD_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

async fn write_zip_bytes(dest: &Path, url: &str, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() < 4 || bytes[0] != b'P' || bytes[1] != b'K' {
        return Err(format!("unduhan bukan zip valid dari {url}"));
    }

    tokio::fs::write(dest, bytes)
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

async fn save_screenshot(page: &Page, dir: &Path, name: &str) -> Option<PathBuf> {
    let path = dir.join(format!("{name}.png"));
    match page
        .save_screenshot(
            ScreenshotParams::builder()
                .format(CaptureScreenshotFormat::Png)
                .full_page(false)
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
