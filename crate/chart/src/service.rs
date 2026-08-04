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

/// Batasi GetCurrentDayChartFromInvezgo ke Senin–Jumat, jam 08:00–12:00 dan 13:30–16:00 (server lokal).
fn require_current_day_chart_hours() -> Result<(), Status> {
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
    const MORNING_START: u32 = 8 * 60;
    const MORNING_END: u32 = 12 * 60 + 1;
    const AFTERNOON_START: u32 = 13 * 60 + 30;
    const AFTERNOON_END: u32 = 16 * 60 + 1;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    if !in_morning && !in_afternoon {
        return Err(Status::failed_precondition(
            "Diluar jam 08:00-12:00 dan 13:30-16:00",
        ));
    }
    Ok(())
}

fn minutes_now() -> u32 {
    let now = Local::now();
    now.hour() * 60 + now.minute()
}

/// Setelah 08:15 volume=0 → anggap market libur untuk code tersebut.
fn can_detect_intraday_holiday() -> bool {
    minutes_now() >= 8 * 60 + 15
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

    fn log_rpc_debug(
        rpc_name: &str,
        user_name: &str,
        started: std::time::Instant,
        detail: &str,
    ) {
        eprintln!(
            "{rpc_name} {user_name} {}ms - {detail}",
            started.elapsed().as_millis()
        );
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
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!("{field} harus format YYYY-MM-DD"))
        })?;
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

        let mut detail = String::new();

        let result: Result<Response<GetCurrentDayChartFromInvezgoResponse>, Status> = async {
            require_current_day_chart_hours()?;
            let code = Self::normalize_code(&request.into_inner().code)?;

            match self.cache.is_intraday_holiday(&code).await {
                Ok(true) => {
                    detail = format!("{code} holiday cache");
                    return Ok(Response::new(holiday_response(&code)));
                }
                Ok(false) => {}
                Err(error) => {
                    detail = format!("holiday cache error: {error}");
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
                        detail = format!("{code} volume=0 → hari libur");
                        return Ok(Response::new(holiday_response(&code)));
                    }
                    detail = format!("{code} close={}", data.close);
                    Ok(Response::new(data))
                }
                Err(error) => {
                    detail = format!("intraday error: {error}");
                    Ok(Response::new(GetCurrentDayChartFromInvezgoResponse {
                        success: false,
                        message: error,
                        ..Default::default()
                    }))
                }
            }
        }
        .await;

        if detail.is_empty() {
            if let Err(ref status) = result {
                detail = status.message().to_string();
            }
        }

        Self::log_rpc_debug(
            "GetCurrentDayChartFromInvezgo",
            &user_name,
            started,
            &detail,
        );
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

        eprintln!(
            "\x1b[32mGetHistoryChartFromInvezgo {user_name} {}ms - {cache_detail}\x1b[0m",
            started.elapsed().as_millis()
        );
        result
    }
}
