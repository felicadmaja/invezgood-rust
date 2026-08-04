use std::sync::Arc;

use chrono::{Datelike, Local, Timelike};
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::cache::ChartCache;
use crate::pb::chart_server::Chart;
use crate::pb::{
    GetCurrentDayChartFromInvezgoRequest, GetCurrentDayChartFromInvezgoResponse,
    GetHistoryChartFromInvezgoRequest, GetHistoryChartFromInvezgoResponse,
};

enum CurrentDayMode {
    /// Jam operasional: hit API live.
    Live,
    /// Setelah 16:00: cache EOD, miss → API 1x.
    EodCache,
}

/// Senin–Jumat: jam operasional live, atau setelah 16:00 via cache EOD.
fn require_current_day_chart_access() -> Result<CurrentDayMode, Status> {
    let now = Local::now();
    match now.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => {
            return Err(Status::failed_precondition(
                "Diluar hari operasional Senin-Jumat",
            ));
        }
        _ => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 9 * 60;
    const MORNING_END: u32 = 12 * 60 + 1;
    const AFTERNOON_START: u32 = 13 * 60 + 30;
    const AFTERNOON_END: u32 = 16 * 60 + 1;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    if in_morning || in_afternoon {
        return Ok(CurrentDayMode::Live);
    }
    // > 16:00 (setelah jam operasional sore)
    if mins > 16 * 60 {
        return Ok(CurrentDayMode::EodCache);
    }
    Err(Status::failed_precondition(
        "Diluar jam 09:00-12:00 dan 13:30-16:00",
    ))
}

fn minutes_now() -> u32 {
    let now = Local::now();
    now.hour() * 60 + now.minute()
}

/// Setelah 09:15 volume=0 → anggap market libur untuk code tersebut.
fn can_detect_intraday_holiday() -> bool {
    minutes_now() >= 9 * 60 + 15
}

fn holiday_response(code: &str) -> GetCurrentDayChartFromInvezgoResponse {
    GetCurrentDayChartFromInvezgoResponse {
        code: code.to_string(),
        success: false,
        message: "hari libur".to_string(),
        ..Default::default()
    }
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

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
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
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let mut code_log = String::new();
        let mut cache_hit = false;

        let result: Result<Response<GetCurrentDayChartFromInvezgoResponse>, Status> = async {
            let mode = require_current_day_chart_access()?;
            let code = Self::normalize_code(&request.into_inner().code)?;
            code_log = code.clone();

            match self.cache.is_intraday_holiday(&code).await {
                Ok(true) => {
                    return Ok(Response::new(holiday_response(&code)));
                }
                Ok(false) => {}
                Err(error) => {
                    return Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                        code: code.clone(),
                        success: false,
                        message: error,
                        ..Default::default()
                    }));
                }
            }

            if matches!(mode, CurrentDayMode::EodCache) {
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
                        if can_detect_intraday_holiday() && data.volume == 0 {
                            if let Err(error) = self.cache.mark_intraday_holiday(&code).await {
                                eprintln!(
                                    "GetCurrentDayChartFromInvezgo mark holiday {code} gagal: {error}"
                                );
                            }
                            return Ok(Response::new(holiday_response(&code)));
                        }
                        if let Err(error) = self.cache.set_intraday_eod(&code, &data).await {
                            eprintln!(
                                "GetCurrentDayChartFromInvezgo set eod {code} gagal: {error}"
                            );
                        }
                        code_log = format!("{code} intraday eod MISS — GET Invezgo");
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
                        if can_detect_intraday_holiday() && data.volume == 0 {
                            if let Err(error) = self.cache.mark_intraday_holiday(&code).await {
                                eprintln!(
                                    "GetCurrentDayChartFromInvezgo mark holiday {code} gagal: {error}"
                                );
                            }
                            return Ok(Response::new(holiday_response(&code)));
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
        if cache_hit {
            eprintln!("GetCurrentDayChartFromInvezgo {user_name} {elapsed}ms - {code_log}");
        } else {
            eprintln!(
                "\x1b[32mGetCurrentDayChartFromInvezgo {user_name} {elapsed}ms - {code_log}\x1b[0m"
            );
        }
        result
    }

    async fn get_history_chart_from_invezgo(
        &self,
        request: Request<GetHistoryChartFromInvezgoRequest>,
    ) -> Result<Response<GetHistoryChartFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

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
}
