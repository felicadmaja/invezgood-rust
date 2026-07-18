//! Bandarmology via API `exodus.stockbit.com/marketdetectors/{CODE}` (bukan scrape DOM).
//! Bearer diambil dari sesi browser setelah login. `to` = kemarin; throttle bila rate-limit habis.

use chrono::{Duration, Months, NaiveDate};
use chromiumoxide::page::Page;
use futures_util::stream::{self, StreamExt};
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use stockbit_browser::extract_stockbit_bearer;
use tokio::sync::Semaphore;
use tokio::time::sleep;

const API_BASE: &str = "https://exodus.stockbit.com/marketdetectors";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Kolom period yang diisi dari API (urutan insert).
const PERIOD_SPECS: &[(&str, PeriodKind)] = &[
    ("d_7", PeriodKind::Days(7)),
    ("d_14", PeriodKind::Days(14)),
    ("M_1", PeriodKind::Months(1)),
    ("M_3", PeriodKind::Months(3)),
    ("M_6", PeriodKind::Months(6)),
    ("M_12", PeriodKind::Months(12)),
    ("Y_3", PeriodKind::Years(3)),
    ("Y_5", PeriodKind::Years(5)),
    ("Y_10", PeriodKind::Years(10)),
    ("Y_15", PeriodKind::Years(15)),
];

#[derive(Clone, Copy)]
enum PeriodKind {
    Days(i64),
    Months(u32),
    Years(u32),
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyTopStats {
    pub volume: i64,
    pub percent: f64,
    pub rp_b: i64,
    pub acc_dist: String,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyBrokerBuy {
    pub broker_code: String,
    pub buy_volume: String,
    pub buy_lot: String,
    pub buy_avg: i64,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyBrokerSell {
    pub broker_code: String,
    pub sell_volume: String,
    pub sell_lot: String,
    pub sell_avg: i64,
}

#[derive(Debug, Clone, SerializeValue, Deserialize)]
pub struct BandarmologyDay {
    pub top_1: BandarmologyTopStats,
    pub top_3: BandarmologyTopStats,
    pub top_5: BandarmologyTopStats,
    pub average: BandarmologyTopStats,
    pub net_volume: i64,
    pub net_value: String,
    pub average_rp: i64,
    pub broker_buy: Vec<BandarmologyBrokerBuy>,
    pub broker_sell: Vec<BandarmologyBrokerSell>,
}

#[derive(Debug, Clone)]
pub struct BandarmologyPeriods {
    pub d_7: BandarmologyDay,
    pub d_14: BandarmologyDay,
    pub m_1: BandarmologyDay,
    pub m_3: BandarmologyDay,
    pub m_6: BandarmologyDay,
    pub m_12: BandarmologyDay,
    pub y_3: BandarmologyDay,
    pub y_5: BandarmologyDay,
    pub y_10: BandarmologyDay,
    pub y_15: BandarmologyDay,
}

#[derive(Debug, DeserializeRow)]
struct CodeNameRow {
    code_name: String,
}

const TOKEN_SEGMENTS: usize = 16;
const TOKEN_SCAN_PAGE_SIZE: i32 = 100;

fn token_segment_start(seg: usize, num_seg: usize) -> i64 {
    if seg == 0 {
        i64::MIN
    } else {
        let span = (i64::MAX as i128) - (i64::MIN as i128);
        (i64::MIN as i128 + (span * seg as i128) / num_seg as i128) as i64
    }
}

fn token_segment_end(seg: usize, num_seg: usize) -> i64 {
    if seg + 1 == num_seg {
        i64::MAX
    } else {
        token_segment_start(seg + 1, num_seg).saturating_sub(1)
    }
}

fn empty_top() -> BandarmologyTopStats {
    BandarmologyTopStats {
        volume: 0,
        percent: 0.0,
        rp_b: 0,
        acc_dist: String::new(),
    }
}

fn empty_day() -> BandarmologyDay {
    BandarmologyDay {
        top_1: empty_top(),
        top_3: empty_top(),
        top_5: empty_top(),
        average: empty_top(),
        net_volume: 0,
        net_value: String::new(),
        average_rp: 0,
        broker_buy: Vec::new(),
        broker_sell: Vec::new(),
    }
}

/// `to` = kemarin relatif terhadap `today` (hari kalender lokal scrape).
pub fn bandarmology_to_date(today: NaiveDate) -> NaiveDate {
    today - Duration::days(1)
}

fn period_from_to(to: NaiveDate, kind: PeriodKind) -> (NaiveDate, NaiveDate) {
    let from = match kind {
        PeriodKind::Days(d) => to - Duration::days(d),
        PeriodKind::Months(m) => to
            .checked_sub_months(Months::new(m))
            .unwrap_or(to),
        PeriodKind::Years(y) => to
            .checked_sub_months(Months::new(y.saturating_mul(12)))
            .unwrap_or(to),
    };
    (from, to)
}

/// Ambil daftar `code_name` dari tabel `emiten_list` via token-ring scan.
pub async fn fetch_emiten_list_code_names(
    session: &Session,
    keyspace: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut scan = session
        .prepare(format!(
            "SELECT code_name FROM {keyspace}.emiten_list \
             WHERE token(code_name) >= ? AND token(code_name) <= ?"
        ))
        .await?;
    scan.set_page_size(TOKEN_SCAN_PAGE_SIZE);

    let mut names = BTreeSet::new();
    for seg in 0..TOKEN_SEGMENTS {
        let start = token_segment_start(seg, TOKEN_SEGMENTS);
        let end = token_segment_end(seg, TOKEN_SEGMENTS);
        let pager = session.execute_iter(scan.clone(), (start, end)).await?;
        let mut rows = pager.rows_stream::<CodeNameRow>()?;
        while let Some(row) = rows.next().await {
            let CodeNameRow { code_name } = row?;
            let n = code_name.trim().to_ascii_uppercase();
            if !n.is_empty() {
                names.insert(n);
            }
        }
    }
    Ok(names.into_iter().collect())
}

/// Alias kompatibilitas: daftar emiten dari `emiten_list` (bukan MV trending harian).
pub async fn fetch_today_emiten_names(
    session: &Session,
    keyspace: &str,
    _today: NaiveDate,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    fetch_emiten_list_code_names(session, keyspace).await
}

fn bandarmology_agg(today: NaiveDate, emiten: &str) -> String {
    format!("{}_{emiten}", today.format("%Y-%m-%d"))
}

/// Kunci partition bandarmology hari ini untuk emiten, mis. `2026-07-17_BBCA`.
pub fn bandarmology_agg_key(today: NaiveDate, emiten: &str) -> String {
    bandarmology_agg(today, emiten.trim().to_ascii_uppercase().as_str())
}

async fn bandarmology_exists(
    session: &Session,
    exists_stmt: &scylla::statement::prepared::PreparedStatement,
    agg: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let result = session
        .execute_unpaged(exists_stmt, (agg,))
        .await?
        .into_rows_result()?;
    Ok(result.rows_num() > 0)
}

/// `true` bila baris `bandarmology` untuk agg hari ini + emiten sudah ada.
pub async fn bandarmology_exists_for_today(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<bool, String> {
    let agg = bandarmology_agg_key(today, emiten);
    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await
        .map_err(|e| e.to_string())?;
    bandarmology_exists(session, &exists_stmt, &agg)
        .await
        .map_err(|e| e.to_string())
}

// --- API response mapping ---------------------------------------------------

#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    #[serde(default)]
    data: Option<ApiData>,
}

#[derive(Debug, Deserialize)]
struct ApiData {
    #[serde(default)]
    bandar_detector: Option<ApiBandarDetector>,
    #[serde(default)]
    broker_summary: Option<ApiBrokerSummary>,
}

#[derive(Debug, Deserialize)]
struct ApiBandarDetector {
    #[serde(default)]
    average: f64,
    #[serde(default)]
    avg: Option<ApiTopStats>,
    #[serde(default)]
    top1: Option<ApiTopStats>,
    #[serde(default)]
    top3: Option<ApiTopStats>,
    #[serde(default)]
    top5: Option<ApiTopStats>,
    #[serde(default)]
    volume: f64,
    #[serde(default)]
    value: f64,
}

#[derive(Debug, Deserialize)]
struct ApiTopStats {
    #[serde(default)]
    accdist: String,
    #[serde(default)]
    amount: f64,
    #[serde(default)]
    percent: f64,
    #[serde(default)]
    vol: f64,
}

#[derive(Debug, Deserialize)]
struct ApiBrokerSummary {
    #[serde(default)]
    brokers_buy: Vec<ApiBrokerBuy>,
    #[serde(default)]
    brokers_sell: Vec<ApiBrokerSell>,
}

#[derive(Debug, Deserialize)]
struct ApiBrokerBuy {
    #[serde(default)]
    netbs_broker_code: String,
    #[serde(default)]
    blot: String,
    #[serde(default)]
    #[allow(dead_code)]
    blotv: String,
    /// Nilai beli — dipakai untuk `buy_volume` di DB.
    #[serde(default)]
    bval: String,
    #[serde(default)]
    netbs_buy_avg_price: String,
}

#[derive(Debug, Deserialize)]
struct ApiBrokerSell {
    #[serde(default)]
    netbs_broker_code: String,
    #[serde(default)]
    slot: String,
    #[serde(default)]
    slotv: String,
    #[serde(default)]
    netbs_sell_avg_price: String,
}

fn amount_to_rp_b(amount: f64) -> i64 {
    // Samakan konvensi UI lama: Rp miliar × 1000 sebagai bigint.
    (amount / 1_000_000_000.0 * 1000.0).round() as i64
}

fn parse_avg_price(s: &str) -> i64 {
    s.trim().parse::<f64>().unwrap_or(0.0).round() as i64
}

/// Parse `"2.349649e+08"` (atau angka biasa) → string integer `"234964900"`.
fn parse_sci_to_int_string(s: &str) -> String {
    let n = s.trim().parse::<f64>().unwrap_or(0.0).round() as i64;
    n.to_string()
}

fn map_top(stats: Option<&ApiTopStats>) -> BandarmologyTopStats {
    let Some(s) = stats else {
        return empty_top();
    };
    BandarmologyTopStats {
        volume: s.vol.round() as i64,
        percent: s.percent,
        rp_b: amount_to_rp_b(s.amount),
        acc_dist: s.accdist.clone(),
    }
}

fn map_api_day(data: &ApiData) -> BandarmologyDay {
    let bd = data.bandar_detector.as_ref();
    let buy = data
        .broker_summary
        .as_ref()
        .map(|s| s.brokers_buy.as_slice())
        .unwrap_or(&[]);
    let sell = data
        .broker_summary
        .as_ref()
        .map(|s| s.brokers_sell.as_slice())
        .unwrap_or(&[]);

    BandarmologyDay {
        top_1: map_top(bd.and_then(|b| b.top1.as_ref())),
        top_3: map_top(bd.and_then(|b| b.top3.as_ref())),
        top_5: map_top(bd.and_then(|b| b.top5.as_ref())),
        average: map_top(bd.and_then(|b| b.avg.as_ref())),
        net_volume: bd.map(|b| b.volume.round() as i64).unwrap_or(0),
        net_value: bd
            .map(|b| {
                if b.value.abs() >= 1_000_000_000.0 {
                    format!("{:.3} B", b.value / 1_000_000_000.0)
                } else {
                    format!("{:.0}", b.value)
                }
            })
            .unwrap_or_default(),
        average_rp: bd.map(|b| b.average.round() as i64).unwrap_or(0),
        broker_buy: buy
            .iter()
            .filter(|r| !r.netbs_broker_code.trim().is_empty())
            .map(|r| BandarmologyBrokerBuy {
                broker_code: r.netbs_broker_code.trim().to_string(),
                buy_volume: parse_sci_to_int_string(&r.bval),
                buy_lot: r.blot.clone(),
                buy_avg: parse_avg_price(&r.netbs_buy_avg_price),
            })
            .collect(),
        broker_sell: sell
            .iter()
            .filter(|r| !r.netbs_broker_code.trim().is_empty())
            .map(|r| BandarmologyBrokerSell {
                broker_code: r.netbs_broker_code.trim().to_string(),
                sell_volume: r.slotv.clone(),
                sell_lot: r.slot.clone(),
                sell_avg: parse_avg_price(&r.netbs_sell_avg_price),
            })
            .collect(),
    }
}

struct RateLimitState {
    limit: Option<i64>,
    remaining: Option<i64>,
    reset_secs: u64,
}

impl RateLimitState {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let limit = headers
            .get("x-rate-limit-limit")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let remaining = headers
            .get("x-rate-limit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let reset_secs = headers
            .get("x-rate-limit-reset")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Self {
            limit,
            remaining,
            reset_secs,
        }
    }

    fn log_line(&self) -> String {
        format!(
            "x-rate-limit-limit={} x-rate-limit-remaining={} x-rate-limit-reset={}",
            self.limit
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            self.remaining
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            self.reset_secs
        )
    }
}

async fn throttle_if_needed(state: &RateLimitState) {
    match state.remaining {
        Some(r) if r <= 0 => {
            let wait = state.reset_secs.max(2);
            println!(
                "Bandarmology API: rate-limit remaining=0 — throttle {wait}s... ({})",
                state.log_line()
            );
            sleep(StdDuration::from_secs(wait)).await;
        }
        Some(r) if r <= 2 => {
            let wait = state.reset_secs.max(1);
            println!(
                "Bandarmology API: rate-limit remaining={r} — throttle {wait}s... ({})",
                state.log_line()
            );
            sleep(StdDuration::from_secs(wait)).await;
        }
        _ => {}
    }
}

const API_TIMEOUT_MAX_RETRIES: u32 = 5;
/// Berapa emiten yang boleh fetch+retry bersamaan (retry timeout tidak menahan emiten lain).
const BANDARMOLOGY_EMITEN_CONCURRENCY: usize = 2;

fn is_retryable_http_status(code: u16) -> bool {
    // Hanya 5xx / network-ish server errors. Semua 4xx → abort app (hindari blokir).
    matches!(code, 500 | 502 | 503 | 504 | 522 | 524)
}

fn timeout_retry_wait_secs(retry_n: u32) -> u64 {
    // Delay memanjang: 2s, 4s, 6s, ... hingga retry ke-10 → 20s.
    2u64.saturating_mul(retry_n.max(1) as u64)
}

async fn fetch_marketdetector_day(
    client: &reqwest::Client,
    bearer: &str,
    emiten: &str,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(BandarmologyDay, RateLimitState), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!(
        "{API_BASE}/{emiten}?from={}&to={}&transaction_type=TRANSACTION_TYPE_NET\
         &market_board=MARKET_BOARD_REGULER&investor_type=INVESTOR_TYPE_ALL&limit=25",
        from.format("%Y-%m-%d"),
        to.format("%Y-%m-%d"),
    );

    let mut timeout_retries = 0u32;
    loop {
        let resp = match client
            .get(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {bearer}"),
            )
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::ORIGIN, "https://stockbit.com")
            .header(reqwest::header::REFERER, "https://stockbit.com/")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .timeout(StdDuration::from_secs(60))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                timeout_retries += 1;
                if timeout_retries > API_TIMEOUT_MAX_RETRIES {
                    return Err(format!(
                        "marketdetectors {emiten} {from}..{to}: network/timeout gagal setelah {API_TIMEOUT_MAX_RETRIES} retry ({e})"
                    )
                    .into());
                }
                let wait = timeout_retry_wait_secs(timeout_retries);
                eprintln!(
                    "Bandarmology API network error untuk {emiten} {from}..{to}: {e} — retry {timeout_retries}/{API_TIMEOUT_MAX_RETRIES} setelah {wait}s"
                );
                sleep(StdDuration::from_secs(wait)).await;
                continue;
            }
        };

        let status = resp.status();
        let rate = RateLimitState::from_headers(resp.headers());
        println!("  API {emiten} {from}..{to} → HTTP {status} | {}", rate.log_line());

        // Semua 4xx (termasuk 401/403/429): hentikan worker + resume PM2.
        crate::http_abort::abort_app_if_http_4xx(
            status,
            &format!("marketdetectors {emiten} {from}..{to}"),
        );

        if is_retryable_http_status(status.as_u16()) && !status.is_success() {
            timeout_retries += 1;
            if timeout_retries > API_TIMEOUT_MAX_RETRIES {
                let body = resp.text().await.unwrap_or_default();
                return Err(format!(
                    "marketdetectors {emiten} {from}..{to}: HTTP {status} setelah {API_TIMEOUT_MAX_RETRIES} retry — {body}"
                )
                .into());
            }
            let wait = timeout_retry_wait_secs(timeout_retries);
            eprintln!(
                "Bandarmology API HTTP {status} untuk {emiten} {from}..{to} — retry {timeout_retries}/{API_TIMEOUT_MAX_RETRIES} setelah {wait}s"
            );
            // Body tidak perlu dibaca penuh; drop response lalu tunggu.
            drop(resp);
            sleep(StdDuration::from_secs(wait)).await;
            continue;
        }

        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "marketdetectors {emiten} {from}..{to}: HTTP {status} {body}"
            )
            .into());
        }

        let envelope: ApiEnvelope = resp.json().await?;
        let day = envelope
            .data
            .as_ref()
            .map(map_api_day)
            .unwrap_or_else(empty_day);
        return Ok((day, rate));
    }
}

async fn fetch_all_periods_for_emiten(
    client: &reqwest::Client,
    bearer: &str,
    emiten: &str,
    to: NaiveDate,
) -> Result<BandarmologyPeriods, Box<dyn std::error::Error + Send + Sync>> {
    let mut days: Vec<BandarmologyDay> = Vec::with_capacity(PERIOD_SPECS.len());
    for (col, kind) in PERIOD_SPECS {
        let (from, to) = period_from_to(to, *kind);
        match fetch_marketdetector_day(client, bearer, emiten, from, to).await {
            Ok((day, rate)) => {
                println!(
                    "  {col}: {from}..{to} net_volume={} brokers_buy={}",
                    day.net_volume,
                    day.broker_buy.len(),
                );
                days.push(day);
                throttle_if_needed(&rate).await;
            }
            Err(e) => {
                let msg = e.to_string();
                eprintln!("  Gagal API {col} untuk {emiten} ({from}..{to}): {msg}");
                if msg.contains("unauthorized") || msg.contains("401") || msg.contains("403") {
                    return Err(format!(
                        "Abort bandarmology: Bearer ditolak API ({msg}). Perbaiki extract token iss=STOCKBIT."
                    )
                    .into());
                }
                days.push(empty_day());
                // Jeda kecil agar tidak spam saat error beruntun.
                sleep(StdDuration::from_millis(400)).await;
            }
        }
    }
    while days.len() < PERIOD_SPECS.len() {
        days.push(empty_day());
    }
    Ok(BandarmologyPeriods {
        d_7: days[0].clone(),
        d_14: days[1].clone(),
        m_1: days[2].clone(),
        m_3: days[3].clone(),
        m_6: days[4].clone(),
        m_12: days[5].clone(),
        y_3: days[6].clone(),
        y_5: days[7].clone(),
        y_10: days[8].clone(),
        y_15: days[9].clone(),
    })
}

async fn insert_bandarmology(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
    p: &BandarmologyPeriods,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let agg = bandarmology_agg(today, emiten);
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.bandarmology (\
                agg_tahun_bulan_tanggal_emiten_name, \
                emiten_name, \
                tahun_bulan_tanggal, \
                d_7, d_14, \"M_1\", \"M_3\", \"M_6\", \"M_12\", \
                \"Y_3\", \"Y_5\", \"Y_10\", \"Y_15\"\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (
                agg.as_str(),
                emiten,
                today,
                &p.d_7,
                &p.d_14,
                &p.m_1,
                &p.m_3,
                &p.m_6,
                &p.m_12,
                &p.y_3,
                &p.y_5,
                &p.y_10,
                &p.y_15,
            ),
        )
        .await?;
    Ok(())
}

/// Fetch API Bandar Detector untuk satu emiten bila agg hari ini belum ada.
/// `page` dipakai hanya untuk ekstrak Bearer setelah login.
pub async fn scrape_bandarmology_for_code_if_missing(
    page: &Page,
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<bool, String> {
    let code = emiten.trim().to_ascii_uppercase();
    if bandarmology_exists_for_today(session, keyspace, today, &code).await? {
        let agg = bandarmology_agg_key(today, &code);
        println!("Skip {code}: bandarmology sudah ada (agg={agg}).");
        return Ok(false);
    }

    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| e.to_string())?;
    let client = reqwest::Client::new();
    let to = bandarmology_to_date(today);

    println!("\n=== Bandarmology API on-demand emiten={code} (to={to}) ===");
    let periods = fetch_all_periods_for_emiten(&client, &bearer, &code, to)
        .await
        .map_err(|e| e.to_string())?;
    insert_bandarmology(session, keyspace, today, &code, &periods)
        .await
        .map_err(|e| e.to_string())?;
    println!("OK: bandarmology insert {code} (on-demand API).");
    Ok(true)
}

/// Marketdetectors API untuk setiap emiten → insert Scylla (d_7..Y_15).
/// Bearer dari sesi browser; throttle otomatis saat `x-rate-limit-remaining` hampir habis.
/// Fetch+retry per emiten dijalankan di task paralel (concurrency terbatas) agar retry timeout
/// satu emiten tidak menahan emiten berikutnya.
pub async fn scrape_and_insert_bandarmology(
    page: &Page,
    session: &Arc<Session>,
    keyspace: &str,
    today: NaiveDate,
    emitens: &[String],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if emitens.is_empty() {
        println!("Tidak ada emiten untuk bandarmology.");
        return Ok(0);
    }

    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;

    let mut todo: Vec<String> = Vec::new();
    let mut skipped = 0usize;
    for emiten in emitens {
        let agg = bandarmology_agg(today, emiten);
        if bandarmology_exists(session, &exists_stmt, &agg).await? {
            println!("Skip {emiten}: bandarmology sudah ada (agg={agg}).");
            skipped += 1;
        } else {
            todo.push(emiten.clone());
        }
    }
    println!(
        "Bandarmology API: {} perlu fetch, {} sudah ada (skip). concurrency={}",
        todo.len(),
        skipped,
        BANDARMOLOGY_EMITEN_CONCURRENCY
    );
    if todo.is_empty() {
        return Ok(0);
    }

    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    println!(
        "Bandarmology API: Bearer OK (len={}), to={} (kemarin dari {today})",
        bearer.len(),
        bandarmology_to_date(today)
    );

    let _ = page
        .evaluate(
            r#"(() => {
                try {
                    return window.__sbCapturedBearer
                        ? ('captured_len=' + window.__sbCapturedBearer.length)
                        : 'captured_empty';
                } catch (_) { return 'captured_err'; }
            })()"#,
        )
        .await;

    let client = Arc::new(reqwest::Client::new());
    let bearer = Arc::new(bearer);
    let session = Arc::clone(session);
    let keyspace = keyspace.to_string();
    let to = bandarmology_to_date(today);
    let total = todo.len();
    let sem = Arc::new(Semaphore::new(BANDARMOLOGY_EMITEN_CONCURRENCY));

    let results: Vec<Result<bool, String>> = stream::iter(todo.into_iter().enumerate())
        .map(|(idx, emiten)| {
            let client = Arc::clone(&client);
            let bearer = Arc::clone(&bearer);
            let session = Arc::clone(&session);
            let keyspace = keyspace.clone();
            let sem = Arc::clone(&sem);
            async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|e| format!("semaphore: {e}"))?;
                println!(
                    "\n=== Bandarmology API [{}/{}] emiten={} (parallel worker) ===",
                    idx + 1,
                    total,
                    emiten
                );
                match fetch_all_periods_for_emiten(client.as_ref(), bearer.as_str(), &emiten, to)
                    .await
                {
                    Ok(periods) => {
                        if let Err(e) =
                            insert_bandarmology(session.as_ref(), &keyspace, today, &emiten, &periods)
                                .await
                        {
                            eprintln!("Gagal insert bandarmology {emiten}: {e}");
                            Ok(false)
                        } else {
                            println!("OK: bandarmology insert {emiten}");
                            Ok(true)
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        eprintln!("Skip {emiten}: gagal fetch periods ({msg})");
                        if msg.contains("Abort bandarmology") || msg.contains("unauthorized") {
                            Err(msg)
                        } else {
                            Ok(false)
                        }
                    }
                }
            }
        })
        .buffer_unordered(BANDARMOLOGY_EMITEN_CONCURRENCY)
        .collect()
        .await;

    let mut ok = 0usize;
    for r in results {
        match r {
            Ok(true) => ok += 1,
            Ok(false) => {}
            Err(msg) => {
                return Err(msg.into());
            }
        }
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn d7_range_matches_user_example() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 18).unwrap();
        let to = bandarmology_to_date(today);
        let (from, to2) = period_from_to(to, PeriodKind::Days(7));
        assert_eq!(to, NaiveDate::from_ymd_opt(2026, 7, 17).unwrap());
        assert_eq!(to2, to);
        assert_eq!(from, NaiveDate::from_ymd_opt(2026, 7, 10).unwrap());
    }

    #[test]
    fn d14_range_matches_user_example() {
        let to = NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        let (from, _) = period_from_to(to, PeriodKind::Days(14));
        assert_eq!(from, NaiveDate::from_ymd_opt(2026, 7, 3).unwrap());
    }

    #[test]
    fn parse_bval_scientific_to_int_string() {
        assert_eq!(parse_sci_to_int_string("2.349649e+08"), "234964900");
        assert_eq!(parse_sci_to_int_string("12345"), "12345");
        assert_eq!(parse_sci_to_int_string(""), "0");
    }
}
