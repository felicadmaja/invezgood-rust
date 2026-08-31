use std::sync::Arc;

use chrono::{Datelike, Local, Timelike};
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

/// Senin–Jumat sebelum 09:00 (menit lokal < 540).
fn is_pre_market_weekday(weekday: chrono::Weekday, hour: u32, minute: u32) -> bool {
    if matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun) {
        return false;
    }
    hour * 60 + minute < 9 * 60
}

/// Senin–Kamis: live 09:00–12:00 & 13:30–16:00. Jumat: 09:00–11:30 & 14:00–16:00.
/// Senin–Jumat 00:00–09:00: PreMarketClosed. Selain itu (istirahat, setelah 16:00, Sabtu/Minggu): Cached.
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
}

#[tonic::async_trait]
impl Chart for ChartService {
    async fn get_current_day_chart_from_invezgo(
        &self,
        request: Request<GetCurrentDayChartFromInvezgoRequest>,
    ) -> Result<Response<GetCurrentDayChartFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let Some(user_name) = self
            .resolve_user_name(
                "GetCurrentDayChartFromInvezgo",
                started,
                &request,
            )
            .await
        else {
            return Ok(Response::new(GetCurrentDayChartFromInvezgoResponse::default()));
        };

        let mut code_log = String::new();
        let mut cache_hit = false;

        let result: Result<Response<GetCurrentDayChartFromInvezgoResponse>, Status> = async {
            let mode = current_day_chart_mode();
            let code = Self::normalize_code(&request.into_inner().code)?;
            code_log = code.clone();

            let is_holiday =
                market_holiday::is_weekend() || market_holiday::is_national_holiday().await;

            if matches!(mode, CurrentDayMode::PreMarketClosed) && !is_holiday {
                return Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                    code,
                    success: false,
                    message: MARKET_NOT_OPEN_MSG.into(),
                    ..Default::default()
                }));
            }

            let use_cache = matches!(mode, CurrentDayMode::Cached) || is_holiday;

            if use_cache {
                match self.cache.get_intraday_eod(&code).await {
                    Ok(Some((data, detail))) => {
                        code_log = format!("{code} {detail}");
                        cache_hit = true;
                        return Ok(Response::new(data));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                            code: code.clone(),
                            success: false,
                            message: error,
                            ..Default::default()
                        }));
                    }
                }

                match crate::invezgo::fetch_intraday_data(&code).await {
                    Ok(data) => {
                        if let Err(error) = self.cache.set_intraday_eod(&code, &data).await {
                            eprintln!(
                                "GetCurrentDayChartFromInvezgo set cache {code} gagal: {error}"
                            );
                        }
                        code_log = format!("{code} intraday cache MISS — GET Invezgo 1x");
                        Ok(Response::new(data))
                    }
                    Err(error) => Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                        success: false,
                        message: error,
                        ..Default::default()
                    })),
                }
            } else {
                match crate::invezgo::fetch_intraday_data(&code).await {
                    Ok(data) => {
                        if let Err(error) = self.cache.set_intraday_eod(&code, &data).await {
                            eprintln!(
                                "GetCurrentDayChartFromInvezgo set cache {code} gagal: {error}"
                            );
                        }
                        Ok(Response::new(data))
                    }
                    Err(error) => Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                        success: false,
                        message: error,
                        ..Default::default()
                    })),
                }
            }
        }
        .await;

        if code_log.is_empty() {
            if let Err(ref status) = result {
                code_log = status.message().to_string();
            }
        }

        let elapsed = started.elapsed().as_millis();
        let ohlc_log = match &result {
            Ok(resp) => format_ohlc(resp.get_ref()),
            Err(_) => String::new(),
        };
        if cache_hit {
            eprintln!(
                "GetCurrentDayChartFromInvezgo {user_name} {elapsed}ms - {code_log}{ohlc_log}"
            );
        } else {
            eprintln!(
                "\x1b[32mGetCurrentDayChartFromInvezgo {user_name} {elapsed}ms - {code_log}{ohlc_log}\x1b[0m"
            );
        }
        result
    }

    async fn get_history_chart_from_invezgo(
        &self,
        request: Request<GetHistoryChartFromInvezgoRequest>,
    ) -> Result<Response<GetHistoryChartFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let Some(user_name) = self
            .resolve_user_name("GetHistoryChartFromInvezgo", started, &request)
            .await
        else {
            return Ok(Response::new(GetHistoryChartFromInvezgoResponse::default()));
        };

        let mut cache_detail = String::new();

        let result: Result<Response<GetHistoryChartFromInvezgoResponse>, Status> = async {
            let req = request.into_inner();
            let code = Self::normalize_code(&req.code)?;
            let from_date = Self::normalize_date("from_date", &req.from_date)?;
            let to_date = Self::normalize_date("to_date", &req.to_date)?;

            match self.cache.get_chart(&code, &from_date, &to_date).await {
                Ok((items, detail)) => {
                    cache_detail = detail;
                    Ok(Response::new(GetHistoryChartFromInvezgoResponse {
                        success: true,
                        message: format!("{} baris", items.len()),
                        items,
                    }))
                }
                Err(error) => {
                    cache_detail = format!("chart error: {error}");
                    Ok(Response::new(GetHistoryChartFromInvezgoResponse {
                        success: false,
                        message: error,
                        items: vec![],
                    }))
                }
            }
        }
        .await;

        let elapsed = started.elapsed().as_millis();
        let is_cache_hit = cache_detail.contains("HIT moka") || cache_detail.contains("HIT redis");
        if is_cache_hit {
            eprintln!("GetHistoryChartFromInvezgo {user_name} {elapsed}ms - {cache_detail}");
        } else {
            eprintln!(
                "\x1b[32mGetHistoryChartFromInvezgo {user_name} {elapsed}ms - {cache_detail}\x1b[0m"
            );
        }
        result
    }

    async fn get_history_ihsg_from_invezgo(
        &self,
        request: Request<GetHistoryIhsgFromInvezgoRequest>,
    ) -> Result<Response<GetHistoryIhsgFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let Some(user_name) = self
            .resolve_user_name("GetHistoryIHSGFromInvezgo", started, &request)
            .await
        else {
            return Ok(Response::new(GetHistoryIhsgFromInvezgoResponse::default()));
        };

        let mut cache_detail = String::new();

        let result: Result<Response<GetHistoryIhsgFromInvezgoResponse>, Status> = async {
            let req = request.into_inner();
            let from_date = Self::normalize_date("from_date", &req.from_date)?;
            let to_date = Self::normalize_date("to_date", &req.to_date)?;

            match self.cache.get_ihsg_chart(&from_date, &to_date).await {
                Ok((items, detail)) => {
                    cache_detail = detail;
                    Ok(Response::new(GetHistoryIhsgFromInvezgoResponse {
                        success: true,
                        message: format!("{} baris", items.len()),
                        items,
                    }))
                }
                Err(error) => {
                    cache_detail = format!("ihsg chart error: {error}");
                    Ok(Response::new(GetHistoryIhsgFromInvezgoResponse {
                        success: false,
                        message: error,
                        items: vec![],
                    }))
                }
            }
        }
        .await;

        let elapsed = started.elapsed().as_millis();
        let is_cache_hit = cache_detail.contains("HIT moka") || cache_detail.contains("HIT redis");
        if is_cache_hit {
            eprintln!("GetHistoryIHSGFromInvezgo {user_name} {elapsed}ms - {cache_detail}");
        } else {
            eprintln!(
                "\x1b[32mGetHistoryIHSGFromInvezgo {user_name} {elapsed}ms - {cache_detail}\x1b[0m"
            );
        }
        result
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
