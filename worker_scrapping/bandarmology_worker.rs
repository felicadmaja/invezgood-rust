//! Bandarmology via API `exodus.stockbit.com/marketdetectors/{CODE}`.
//! Bearer dari sesi browser setelah login.
//!
//! Per emiten:
//! - Bulan berjalan: `from` = awal bulan, `to` = hari ini → upsert (selalu timpa).
//! - Bulan sebelumnya (max 36 / 3 tahun): `from`/`to` = awal–akhir bulan; skip bila baris sudah ada;
//!   hentikan backfill bila 2 bulan berturut-turut sudah ada, atau 2 bulan berturut-turut kosong dari API.
//! - Semua emiten diproses **sequential** (satu worker utama), jeda 100 ms antar emiten, 50 ms antar bulan per emiten.
//! - Bila request API timeout/network error: retry di **background task** tanpa menahan worker utama.

use chrono::{Datelike, Duration, Months, NaiveDate};
use chromiumoxide::page::Page;
use futures_util::StreamExt;
use scylla::client::session::Session;
use scylla::{DeserializeRow, SerializeValue};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use stockbit_browser::extract_stockbit_bearer;
use tokio::time::sleep;

const API_BASE: &str = "https://exodus.stockbit.com/marketdetectors";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const MAX_HISTORICAL_MONTHS: u32 = 36;
const CONSECUTIVE_EMPTY_MONTHS_STOP: usize = 2;
const CONSECUTIVE_SKIP_EXISTING_STOP: usize = 2;
const EMITEN_INTER_DELAY_MS: u64 = 100;
const MONTH_INTER_DELAY_MS: u64 = 50;

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

/// `to` = kemarin relatif terhadap `today` (legacy helper).
pub fn bandarmology_to_date(today: NaiveDate) -> NaiveDate {
    today - Duration::days(1)
}

pub fn tahun_bulan_str(year: i32, month: u32) -> String {
    format!("{year:04}-{month:02}")
}

pub fn agg_tahun_bulan_emiten_name(tahun_bulan: &str, emiten: &str) -> String {
    format!(
        "{}_{}",
        tahun_bulan.trim(),
        emiten.trim().to_ascii_uppercase()
    )
}

fn first_day_of_month(year: i32, month: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    first_day_of_month(year, month)
        .checked_add_months(Months::new(1))
        .and_then(|d| d.pred_opt())
        .unwrap_or_else(|| first_day_of_month(year, month))
}

fn current_month_range(today: NaiveDate) -> (NaiveDate, NaiveDate, String) {
    let from = first_day_of_month(today.year(), today.month());
    let tb = tahun_bulan_str(today.year(), today.month());
    (from, today, tb)
}

fn is_broker_summary_empty(day: &BandarmologyDay) -> bool {
    day.net_volume == 0
        && day.broker_buy.is_empty()
        && day.broker_sell.is_empty()
        && day.top_1.volume == 0
        && day.top_3.volume == 0
        && day.top_5.volume == 0
}

/// Kunci partition bandarmology bulan berjalan untuk emiten, mis. `2026-07_BBCA`.
pub fn bandarmology_agg_key(today: NaiveDate, emiten: &str) -> String {
    agg_tahun_bulan_emiten_name(
        &tahun_bulan_str(today.year(), today.month()),
        emiten,
    )
}

async fn bandarmology_exists_by_agg(
    session: &Session,
    keyspace: &str,
    agg: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_emiten_name \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_emiten_name = ?"
        ))
        .await?;
    bandarmology_exists(session, &exists_stmt, agg).await
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

/// `true` bila baris `bandarmology` untuk agg bulan berjalan + emiten sudah ada.
pub async fn bandarmology_exists_for_today(
    session: &Session,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<bool, String> {
    let agg = bandarmology_agg_key(today, emiten);
    bandarmology_exists_by_agg(session, keyspace, &agg)
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

fn is_retryable_http_status(code: u16) -> bool {
    // Hanya 5xx / network-ish server errors. Semua 4xx → abort app (hindari blokir).
    matches!(code, 500 | 502 | 503 | 504 | 522 | 524)
}

fn timeout_retry_wait_secs(retry_n: u32) -> u64 {
    2u64.saturating_mul(retry_n.max(1) as u64)
}

struct BackgroundInsertCtx {
    session: Arc<Session>,
    keyspace: String,
    tahun_bulan: String,
}

fn spawn_background_fetch_retry(
    client: reqwest::Client,
    bearer: String,
    emiten: String,
    from: NaiveDate,
    to: NaiveDate,
    ctx: BackgroundInsertCtx,
) {
    tokio::spawn(async move {
        println!(
            "  [background] retry API {emiten} {} ({from}..{to})",
            ctx.tahun_bulan
        );
        match fetch_marketdetector_day_blocking_retries(
            &client,
            &bearer,
            &emiten,
            from,
            to,
        )
        .await
        {
            Ok((day, rate)) => {
                throttle_if_needed(&rate).await;
                if is_broker_summary_empty(&day) {
                    println!(
                        "  [background] {emiten} {} kosong — skip insert",
                        ctx.tahun_bulan
                    );
                    return;
                }
                match insert_bandarmology(
                    ctx.session.as_ref(),
                    &ctx.keyspace,
                    &emiten,
                    &ctx.tahun_bulan,
                    &day,
                )
                .await
                {
                    Ok(()) => println!(
                        "  [background] OK insert {emiten} {}",
                        ctx.tahun_bulan
                    ),
                    Err(e) => eprintln!(
                        "  [background] gagal insert {emiten} {}: {e}",
                        ctx.tahun_bulan
                    ),
                }
            }
            Err(e) => eprintln!(
                "  [background] gagal fetch {emiten} {}: {e}",
                ctx.tahun_bulan
            ),
        }
    });
}

fn is_timeout_or_network(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

/// Fetch dengan retry penuh — hanya dipakai background worker (bukan worker utama).
async fn fetch_marketdetector_day_blocking_retries(
    client: &reqwest::Client,
    bearer: &str,
    emiten: &str,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(BandarmologyDay, RateLimitState), Box<dyn std::error::Error + Send + Sync>> {
    let url = marketdetector_url(emiten, from, to);
    let mut timeout_retries = 0u32;
    loop {
        let resp = match send_marketdetector_request(client, bearer, &url).await {
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

        match parse_marketdetector_response(resp, emiten, from, to, &mut timeout_retries).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
    }
}

fn marketdetector_url(emiten: &str, from: NaiveDate, to: NaiveDate) -> String {
    format!(
        "{API_BASE}/{emiten}?from={}&to={}&transaction_type=TRANSACTION_TYPE_NET\
         &market_board=MARKET_BOARD_REGULER&investor_type=INVESTOR_TYPE_ALL&limit=25",
        from.format("%Y-%m-%d"),
        to.format("%Y-%m-%d"),
    )
}

async fn send_marketdetector_request(
    client: &reqwest::Client,
    bearer: &str,
    url: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    client
        .get(url)
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
}

async fn parse_marketdetector_response(
    resp: reqwest::Response,
    emiten: &str,
    from: NaiveDate,
    to: NaiveDate,
    timeout_retries: &mut u32,
) -> Result<Option<(BandarmologyDay, RateLimitState)>, Box<dyn std::error::Error + Send + Sync>> {
    let status = resp.status();
    let rate = RateLimitState::from_headers(resp.headers());
    println!("  API {emiten} {from}..{to} → HTTP {status} | {}", rate.log_line());

    crate::http_abort::abort_app_if_http_4xx(
        status,
        &format!("marketdetectors {emiten} {from}..{to}"),
    );

    if is_retryable_http_status(status.as_u16()) && !status.is_success() {
        *timeout_retries += 1;
        if *timeout_retries > API_TIMEOUT_MAX_RETRIES {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "marketdetectors {emiten} {from}..{to}: HTTP {status} setelah {API_TIMEOUT_MAX_RETRIES} retry — {body}"
            )
            .into());
        }
        let wait = timeout_retry_wait_secs(*timeout_retries);
        eprintln!(
            "Bandarmology API HTTP {status} untuk {emiten} {from}..{to} — retry {timeout_retries}/{API_TIMEOUT_MAX_RETRIES} setelah {wait}s"
        );
        drop(resp);
        sleep(StdDuration::from_secs(wait)).await;
        return Ok(None);
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
    Ok(Some((day, rate)))
}

/// Worker utama: HTTP retry inline; timeout/network → spawn background retry + lanjut.
async fn fetch_marketdetector_day(
    client: &reqwest::Client,
    bearer: &str,
    emiten: &str,
    from: NaiveDate,
    to: NaiveDate,
    bg_insert: Option<BackgroundInsertCtx>,
) -> Result<(BandarmologyDay, RateLimitState), Box<dyn std::error::Error + Send + Sync>> {
    let url = marketdetector_url(emiten, from, to);
    let mut http_retries = 0u32;
    loop {
        let resp = match send_marketdetector_request(client, bearer, &url).await {
            Ok(r) => r,
            Err(e) if is_timeout_or_network(&e) => {
                if let Some(ctx) = bg_insert {
                    spawn_background_fetch_retry(
                        client.clone(),
                        bearer.to_string(),
                        emiten.to_string(),
                        from,
                        to,
                        ctx,
                    );
                }
                return Err(format!(
                    "marketdetectors {emiten} {from}..{to}: timeout/network — retry di background worker ({e})"
                )
                .into());
            }
            Err(e) => {
                return Err(format!(
                    "marketdetectors {emiten} {from}..{to}: request gagal ({e})"
                )
                .into());
            }
        };

        match parse_marketdetector_response(resp, emiten, from, to, &mut http_retries).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
    }
}

async fn insert_bandarmology(
    session: &Session,
    keyspace: &str,
    emiten: &str,
    tahun_bulan: &str,
    broker_summary: &BandarmologyDay,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let agg = agg_tahun_bulan_emiten_name(tahun_bulan, emiten);
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.bandarmology (\
                agg_tahun_bulan_emiten_name, \
                emiten_name, \
                tahun_bulan, \
                broker_summary\
            ) VALUES (?, ?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(
            &insert,
            (agg.as_str(), emiten, tahun_bulan, broker_summary),
        )
        .await?;
    Ok(())
}

fn is_auth_abort(err: &str) -> bool {
    err.contains("unauthorized") || err.contains("401") || err.contains("403") || err.contains("Abort bandarmology")
}

fn is_background_timeout_err(err: &str) -> bool {
    err.contains("retry di background worker")
}

async fn scrape_emiten_bandarmology(
    client: &reqwest::Client,
    bearer: &str,
    session: Arc<Session>,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let code = emiten.trim().to_ascii_uppercase();
    let mut inserted = 0usize;

    let (cur_from, cur_to, cur_tb) = current_month_range(today);
    println!(
        "  [{code}] bulan berjalan {cur_tb}: {cur_from}..{cur_to} (overwrite)"
    );
    let cur_ctx = BackgroundInsertCtx {
        session: Arc::clone(&session),
        keyspace: keyspace.to_string(),
        tahun_bulan: cur_tb.clone(),
    };
    match fetch_marketdetector_day(
        client,
        bearer,
        &code,
        cur_from,
        cur_to,
        Some(cur_ctx),
    )
    .await
    {
        Ok((day, rate)) => {
            println!(
                "  [{code}] {cur_tb} net_volume={} brokers_buy={}",
                day.net_volume,
                day.broker_buy.len()
            );
            throttle_if_needed(&rate).await;
            insert_bandarmology(session.as_ref(), keyspace, &code, &cur_tb, &day).await?;
            inserted += 1;
        }
        Err(e) => {
            let msg = e.to_string();
            if is_auth_abort(&msg) {
                return Err(e);
            }
            eprintln!("  [{code}] gagal fetch bulan berjalan {cur_tb}: {msg}");
        }
    }

    sleep(StdDuration::from_millis(MONTH_INTER_DELAY_MS)).await;

    let mut empty_streak = 0usize;
    let mut skip_existing_streak = 0usize;
    for offset in 1..=MAX_HISTORICAL_MONTHS {
        if offset > 1 {
            sleep(StdDuration::from_millis(MONTH_INTER_DELAY_MS)).await;
        }
        let Some(month_anchor) = today.checked_sub_months(Months::new(offset)) else {
            break;
        };
        let y = month_anchor.year();
        let m = month_anchor.month();
        let tb = tahun_bulan_str(y, m);
        let agg = agg_tahun_bulan_emiten_name(&tb, &code);

        if bandarmology_exists_by_agg(session.as_ref(), keyspace, &agg).await? {
            skip_existing_streak += 1;
            println!(
                "  [{code}] skip historis {tb}: sudah ada (agg={agg}, streak={skip_existing_streak})"
            );
            if skip_existing_streak >= CONSECUTIVE_SKIP_EXISTING_STOP {
                println!(
                    "  [{code}] hentikan historis: {CONSECUTIVE_SKIP_EXISTING_STOP} bulan berturut-turut sudah ada — lanjut emiten berikutnya"
                );
                break;
            }
            continue;
        }

        skip_existing_streak = 0;

        let from = first_day_of_month(y, m);
        let to = last_day_of_month(y, m);
        println!("  [{code}] historis {tb}: {from}..{to}");

        let hist_ctx = BackgroundInsertCtx {
            session: Arc::clone(&session),
            keyspace: keyspace.to_string(),
            tahun_bulan: tb.clone(),
        };
        let fetch_result = fetch_marketdetector_day(
            client,
            bearer,
            &code,
            from,
            to,
            Some(hist_ctx),
        )
        .await;
        let (day, rate) = match fetch_result {
            Ok(v) => v,
            Err(e) => {
                let msg = e.to_string();
                if is_auth_abort(&msg) {
                    return Err(e);
                }
                eprintln!("  [{code}] gagal fetch {tb}: {msg}");
                if is_background_timeout_err(&msg) {
                    continue;
                }
                empty_streak += 1;
                if empty_streak >= CONSECUTIVE_EMPTY_MONTHS_STOP {
                    println!(
                        "  [{code}] hentikan historis: {CONSECUTIVE_EMPTY_MONTHS_STOP} bulan kosong/gagal berturut-turut"
                    );
                    break;
                }
                continue;
            }
        };
        throttle_if_needed(&rate).await;

        if is_broker_summary_empty(&day) {
            empty_streak += 1;
            println!("  [{code}] {tb} kosong (streak={empty_streak})");
            if empty_streak >= CONSECUTIVE_EMPTY_MONTHS_STOP {
                println!(
                    "  [{code}] hentikan historis: {CONSECUTIVE_EMPTY_MONTHS_STOP} bulan kosong berturut-turut"
                );
                break;
            }
            continue;
        }

        empty_streak = 0;
        insert_bandarmology(session.as_ref(), keyspace, &code, &tb, &day).await?;
        println!(
            "  [{code}] insert historis {tb} net_volume={}",
            day.net_volume
        );
        inserted += 1;
    }

    Ok(inserted)
}

/// Scrape bandarmology untuk satu emiten: bulan berjalan (overwrite) + backfill historis.
/// Returns jumlah baris bulan yang di-upsert.
pub async fn scrape_bandarmology_for_code_if_missing(
    page: &Page,
    session: Arc<Session>,
    keyspace: &str,
    today: NaiveDate,
    emiten: &str,
) -> Result<usize, String> {
    let code = emiten.trim().to_ascii_uppercase();
    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| e.to_string())?;
    let client = reqwest::Client::new();

    println!("\n=== Bandarmology API on-demand emiten={code} (today={today}) ===");
    let inserted = scrape_emiten_bandarmology(
        &client,
        &bearer,
        session,
        keyspace,
        today,
        &code,
    )
    .await
    .map_err(|e| e.to_string())?;
    println!("OK: bandarmology {code} — {inserted} baris bulan di-upsert.");
    Ok(inserted)
}

/// Marketdetectors API per emiten → upsert Scylla per bulan (`broker_summary`).
/// Satu emiten per satu (sequential); timeout API di-retry di background task.
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

    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    println!(
        "Bandarmology API: Bearer OK (len={}), today={today}, emiten={} (sequential)",
        bearer.len(),
        emitens.len(),
    );

    let client = reqwest::Client::new();
    let session = Arc::clone(session);
    let keyspace = keyspace.to_string();
    let total = emitens.len();
    let mut total_inserted = 0usize;
    let mut emitens_ok = 0usize;

    for (idx, emiten) in emitens.iter().enumerate() {
        println!(
            "\n=== Bandarmology API [{}/{}] emiten={} ===",
            idx + 1,
            total,
            emiten
        );
        match scrape_emiten_bandarmology(
            &client,
            &bearer,
            Arc::clone(&session),
            &keyspace,
            today,
            emiten,
        )
        .await
        {
            Ok(n) if n > 0 => {
                emitens_ok += 1;
                total_inserted += n;
            }
            Ok(_) => {}
            Err(e) if is_auth_abort(&e.to_string()) => return Err(e),
            Err(e) => eprintln!("Bandarmology emiten gagal: {e}"),
        }
        if idx + 1 < total {
            sleep(StdDuration::from_millis(EMITEN_INTER_DELAY_MS)).await;
        }
    }

    println!(
        "Bandarmology selesai: {emitens_ok}/{} emiten, {total_inserted} baris bulan di-upsert.",
        emitens.len()
    );
    Ok(emitens_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn current_month_range_example() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 19).unwrap();
        let (from, to, tb) = current_month_range(today);
        assert_eq!(from, NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
        assert_eq!(to, today);
        assert_eq!(tb, "2026-07");
        assert_eq!(
            agg_tahun_bulan_emiten_name(&tb, "BBCA"),
            "2026-07_BBCA"
        );
    }

    #[test]
    fn last_day_of_month_july_2026() {
        assert_eq!(
            last_day_of_month(2026, 7),
            NaiveDate::from_ymd_opt(2026, 7, 31).unwrap()
        );
    }

    #[test]
    fn is_broker_summary_empty_detects_no_data() {
        assert!(is_broker_summary_empty(&empty_day()));
    }

    #[test]
    fn parse_bval_scientific_to_int_string() {
        assert_eq!(parse_sci_to_int_string("2.349649e+08"), "234964900");
        assert_eq!(parse_sci_to_int_string("12345"), "12345");
        assert_eq!(parse_sci_to_int_string(""), "0");
    }
}
