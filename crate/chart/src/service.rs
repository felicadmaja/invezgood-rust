use std::pin::Pin;
use std::sync::Arc;

use chrono::{Datelike, Local, Timelike};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, SessionStore};

use crate::cache::ChartCache;
use crate::pb::chart_server::Chart;
use crate::pb::{
    GetCurrentDayChartFromInvezgoRequest, GetCurrentDayChartFromInvezgoResponse,
    GetHistoryChartFromInvezgoRequest, GetHistoryChartFromInvezgoResponse,
    GetHistoryIhsgFromInvezgoRequest, GetHistoryIhsgFromInvezgoResponse,
};

enum CurrentDayMode {
    /// Jam operasional: GET Invezgo live + simpan snapshot ke cache.
    Live,
    /// Diluar jam operasional / libur: cache Moka→Redis; miss → GET Invezgo 1x lalu simpan.
    Cached,
    /// Senin–Jumat 00:00–09:00 (bukan libur): pasar belum buka.
    PreMarketClosed,
}

const MARKET_NOT_OPEN_MSG: &str = "Market belum buka.";

type ChartStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

async fn stream_once<T: Send + 'static>(item: Result<T, Status>) -> ChartStream<T> {
    let (tx, rx) = mpsc::channel(1);
    let _ = tx.send(item).await;
    Box::pin(ReceiverStream::new(rx))
}

/// Senin–Jumat sebelum 09:00 (menit lokal < 540).
fn is_pre_market_weekday(weekday: chrono::Weekday, hour: u32, minute: u32) -> bool {
    if matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun) {
        return false;
    }
    hour * 60 + minute < 9 * 60
}

/// Senin–Kamis: live 09:00–12:00 & 13:30–16:00. Jumat: 09:00–11:30 & 14:00–16:00.
/// Senin–Jumat 00:00–08:59: PreMarketClosed. Selain itu (istirahat, setelah 16:00, Sabtu/Minggu): Cached.
fn current_day_chart_mode() -> CurrentDayMode {
    let now = Local::now();
    let weekday = now.weekday();
    if matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun) {
        return CurrentDayMode::Cached;
    }

    let hour = now.hour();
    let minute = now.minute();
    if is_pre_market_weekday(weekday, hour, minute) {
        return CurrentDayMode::PreMarketClosed;
    }

    let mins = hour * 60 + minute;
    const MORNING_START: u32 = 9 * 60;
    let in_session = match weekday {
        chrono::Weekday::Fri => {
            const MORNING_END: u32 = 11 * 60 + 30 + 1;
            const AFTERNOON_START: u32 = 14 * 60;
            const AFTERNOON_END: u32 = 16 * 60 + 1;
            (mins >= MORNING_START && mins < MORNING_END)
                || (mins >= AFTERNOON_START && mins < AFTERNOON_END)
        }
        _ => {
            const MORNING_END: u32 = 12 * 60 + 1;
            const AFTERNOON_START: u32 = 13 * 60 + 30;
            const AFTERNOON_END: u32 = 16 * 60 + 1;
            (mins >= MORNING_START && mins < MORNING_END)
                || (mins >= AFTERNOON_START && mins < AFTERNOON_END)
        }
    };
    if in_session {
        CurrentDayMode::Live
    } else {
        CurrentDayMode::Cached
    }
}

fn format_ohlc(d: &GetCurrentDayChartFromInvezgoResponse) -> String {
    format!(
        " O={:.2} H={:.2} L={:.2} C={:.2}",
        d.open, d.high, d.low, d.close
    )
}

pub struct ChartService {
    cache: Arc<ChartCache>,
    auth_sessions: SessionStore,
}

impl ChartService {
    pub fn new(cache: Arc<ChartCache>, auth_sessions: SessionStore) -> Self {
        Self {
            cache,
            auth_sessions,
        }
    }

    async fn resolve_user_name<T>(
        &self,
        rpc_name: &str,
        started: std::time::Instant,
        request: &Request<T>,
    ) -> Option<String> {
        match extract_bearer_token(request) {
            Ok(token) => match validate_session(&self.auth_sessions, &token).await {
                Ok(auth) => Some(auth.nama),
                Err(_) => {
                    eprintln!(
                        "{rpc_name} anonymous {}ms — abaikan (session invalid)",
                        started.elapsed().as_millis()
                    );
                    None
                }
            },
            Err(_) => {
                eprintln!(
                    "{rpc_name} anonymous {}ms — abaikan (tanpa auth)",
                    started.elapsed().as_millis()
                );
                None
            }
        }
    }

    fn normalize_code(raw: &str) -> Result<String, Status> {
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "code tidak valid ({raw}); wajib tepat 4 huruf alphabet"
            )));
        }
        Ok(code)
    }

    fn normalize_date(field: &str, raw: &str) -> Result<String, Status> {
        let value = raw.trim();
        if value.is_empty() {
            return Err(Status::invalid_argument(format!("{field} wajib diisi")));
        }
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| Status::invalid_argument(format!("{field} harus format YYYY-MM-DD")))?;
        Ok(value.to_string())
    }

    async fn fetch_intraday_live_cached(
        &self,
        code: &str,
        code_log: &mut String,
        cache_hit: &mut bool,
    ) -> GetCurrentDayChartFromInvezgoResponse {
        if let Some((data, detail)) = self.cache.get_intraday_live(code).await {
            *code_log = format!("{code} {detail}");
            *cache_hit = true;
            return data;
        }

        *code_log = format!("{code} intraday live MISS — GET Invezgo");
        match crate::invezgo::fetch_intraday_data(code).await {
            Ok(data) => {
                if ChartCache::has_valid_intraday_ohlcv(&data) {
                    self.cache.set_intraday_live(code, &data).await;
                    if let Err(error) = self.cache.set_intraday_eod(code, &data).await {
                        eprintln!(
                            "GetCurrentDayChartFromInvezgo set eod cache {code} gagal: {error}"
                        );
                    }
                }
                data
            }
            Err(error) => GetCurrentDayChartFromInvezgoResponse {
                success: false,
                message: error,
                ..Default::default()
            },
        }
    }

    async fn compute_current_day_chart(
        &self,
        code: String,
        code_log: &mut String,
        cache_hit: &mut bool,
    ) -> GetCurrentDayChartFromInvezgoResponse {
        let mode = current_day_chart_mode();
        *code_log = code.clone();

        let is_holiday =
            market_holiday::is_weekend() || market_holiday::is_national_holiday().await;

        if matches!(mode, CurrentDayMode::PreMarketClosed) && !is_holiday {
            return GetCurrentDayChartFromInvezgoResponse {
                code,
                success: false,
                message: MARKET_NOT_OPEN_MSG.into(),
                ..Default::default()
            };
        }

        let use_cache = matches!(mode, CurrentDayMode::Cached) || is_holiday;

        if use_cache {
            match self.cache.get_intraday_eod(&code).await {
                Ok(Some((data, detail))) => {
                    *code_log = format!("{code} {detail}");
                    *cache_hit = true;
                    return data;
                }
                Ok(None) => {}
                Err(error) => {
                    return GetCurrentDayChartFromInvezgoResponse {
                        code: code.clone(),
                        success: false,
                        message: error,
                        ..Default::default()
                    };
                }
            }

            match crate::invezgo::fetch_intraday_data(&code).await {
                Ok(data) => {
                    if let Err(error) = self.cache.set_intraday_eod(&code, &data).await {
                        eprintln!("GetCurrentDayChartFromInvezgo set cache {code} gagal: {error}");
                    }
                    *code_log = format!("{code} intraday cache MISS — GET Invezgo 1x");
                    data
                }
                Err(error) => GetCurrentDayChartFromInvezgoResponse {
                    success: false,
                    message: error,
                    ..Default::default()
                },
            }
        } else {
            self.fetch_intraday_live_cached(&code, code_log, cache_hit)
                .await
        }
    }
}

#[tonic::async_trait]
impl Chart for ChartService {
    type GetCurrentDayChartFromInvezgoStream = ChartStream<GetCurrentDayChartFromInvezgoResponse>;
    type GetHistoryChartFromInvezgoStream = ChartStream<GetHistoryChartFromInvezgoResponse>;
    type GetHistoryIHSGFromInvezgoStream = ChartStream<GetHistoryIhsgFromInvezgoResponse>;

    async fn get_current_day_chart_from_invezgo(
        &self,
        request: Request<GetCurrentDayChartFromInvezgoRequest>,
    ) -> Result<Response<Self::GetCurrentDayChartFromInvezgoStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetCurrentDayChartFromInvezgo";

        let Some(user_name) = self.resolve_user_name(rpc_name, started, &request).await else {
            return Ok(Response::new(
                stream_once(Ok(GetCurrentDayChartFromInvezgoResponse::default())).await,
            ));
        };

        let code_raw = request.into_inner().code;
        let code = match Self::normalize_code(&code_raw) {
            Ok(c) => c,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };

        let mut code_log = String::new();
        let mut cache_hit = false;
        let response = self
            .compute_current_day_chart(code, &mut code_log, &mut cache_hit)
            .await;

        let elapsed = started.elapsed().as_millis();
        let ohlc_log = format_ohlc(&response);
        if cache_hit {
            eprintln!("{rpc_name} {user_name} {elapsed}ms - {code_log}{ohlc_log}");
        } else {
            eprintln!(
                "\x1b[32m{rpc_name} {user_name} {elapsed}ms - {code_log}{ohlc_log}\x1b[0m"
            );
        }

        Ok(Response::new(stream_once(Ok(response)).await))
    }

    async fn get_history_chart_from_invezgo(
        &self,
        request: Request<GetHistoryChartFromInvezgoRequest>,
    ) -> Result<Response<Self::GetHistoryChartFromInvezgoStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetHistoryChartFromInvezgo";

        let Some(user_name) = self.resolve_user_name(rpc_name, started, &request).await else {
            return Ok(Response::new(
                stream_once(Ok(GetHistoryChartFromInvezgoResponse::default())).await,
            ));
        };

        let req_ref = request.get_ref();
        eprintln!(
            "{rpc_name} invoke {user_name} code={} from={} to={}",
            req_ref.code.trim(),
            req_ref.from_date.trim(),
            req_ref.to_date.trim(),
        );

        let req = request.into_inner();
        let code = match Self::normalize_code(&req.code) {
            Ok(c) => c,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };
        let from_date = match Self::normalize_date("from_date", &req.from_date) {
            Ok(v) => v,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };
        let to_date = match Self::normalize_date("to_date", &req.to_date) {
            Ok(v) => v,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };

        let (response, cache_detail) = match self.cache.get_chart(&code, &from_date, &to_date).await
        {
            Ok((items, detail)) => (
                GetHistoryChartFromInvezgoResponse {
                    success: true,
                    message: format!("{} baris", items.len()),
                    items,
                },
                detail,
            ),
            Err(error) => (
                GetHistoryChartFromInvezgoResponse {
                    success: false,
                    message: error.clone(),
                    items: vec![],
                },
                format!("chart error: {error}"),
            ),
        };

        let elapsed = started.elapsed().as_millis();
        let is_cache_hit = cache_detail.contains("HIT moka") || cache_detail.contains("HIT redis");
        if is_cache_hit {
            eprintln!("{rpc_name} {user_name} {elapsed}ms - {cache_detail}");
        } else {
            eprintln!(
                "\x1b[32m{rpc_name} {user_name} {elapsed}ms - {cache_detail}\x1b[0m"
            );
        }

        Ok(Response::new(stream_once(Ok(response)).await))
    }

    async fn get_history_ihsg_from_invezgo(
        &self,
        request: Request<GetHistoryIhsgFromInvezgoRequest>,
    ) -> Result<Response<Self::GetHistoryIHSGFromInvezgoStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetHistoryIHSGFromInvezgo";

        let Some(user_name) = self.resolve_user_name(rpc_name, started, &request).await else {
            return Ok(Response::new(
                stream_once(Ok(GetHistoryIhsgFromInvezgoResponse::default())).await,
            ));
        };

        let req = request.into_inner();
        let from_date = match Self::normalize_date("from_date", &req.from_date) {
            Ok(v) => v,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };
        let to_date = match Self::normalize_date("to_date", &req.to_date) {
            Ok(v) => v,
            Err(status) => {
                eprintln!(
                    "{rpc_name} {user_name} {}ms",
                    started.elapsed().as_millis()
                );
                return Ok(Response::new(stream_once(Err(status)).await));
            }
        };

        let (response, cache_detail) =
            match self.cache.get_ihsg_chart(&from_date, &to_date).await {
                Ok((items, detail)) => (
                    GetHistoryIhsgFromInvezgoResponse {
                        success: true,
                        message: format!("{} baris", items.len()),
                        items,
                    },
                    detail,
                ),
                Err(error) => (
                    GetHistoryIhsgFromInvezgoResponse {
                        success: false,
                        message: error.clone(),
                        items: vec![],
                    },
                    format!("ihsg chart error: {error}"),
                ),
            };

        let elapsed = started.elapsed().as_millis();
        let is_cache_hit = cache_detail.contains("HIT moka") || cache_detail.contains("HIT redis");
        if is_cache_hit {
            eprintln!("{rpc_name} {user_name} {elapsed}ms - {cache_detail}");
        } else {
            eprintln!(
                "\x1b[32m{rpc_name} {user_name} {elapsed}ms - {cache_detail}\x1b[0m"
            );
        }

        Ok(Response::new(stream_once(Ok(response)).await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Weekday;

    #[test]
    fn pre_market_weekday_before_nine() {
        assert!(is_pre_market_weekday(Weekday::Mon, 8, 59));
        assert!(is_pre_market_weekday(Weekday::Fri, 0, 0));
        assert!(!is_pre_market_weekday(Weekday::Mon, 9, 0));
        assert!(!is_pre_market_weekday(Weekday::Sat, 8, 0));
    }
}
