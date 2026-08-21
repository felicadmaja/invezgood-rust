use std::sync::Arc;

use chrono::{Datelike, Local, NaiveDate, Timelike};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::invezgo;
use crate::intraday_cache::IntradayCache;
use crate::model::agg_code_tahun_bulan_tanggal;
use crate::pb::haka_haki_server::HakaHaki as HakaHakiRpc;
use crate::pb::{
    GetHakaHakiFromInvezgoRequest, GetHakaHakiFromInvezgoResponse,
    GetHakaHakiFromScyllaRequest, GetHakaHakiFromScyllaResponse,
};

const DEFAULT_RANGE: i32 = 5;

enum CurrentDayMode {
    Live,
    EodCache,
}

enum LogSource {
    Cache,
    EodCache,
    Api,
}

pub struct HakaHakiService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
    intraday_cache: Arc<IntradayCache>,
}

impl HakaHakiService {
    pub fn new(
        session: Arc<Session>,
        auth_sessions: SessionStore,
        intraday_cache: Arc<IntradayCache>,
    ) -> Self {
        Self {
            session,
            auth_sessions,
            intraday_cache,
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

    fn parse_trade_date(raw: &str) -> Result<NaiveDate, Status> {
        let value = raw.trim();
        if value.is_empty() {
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal wajib diisi (YYYY-MM-DD)",
            ));
        }
        NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!(
                "tahun_bulan_tanggal tidak valid (harus YYYY-MM-DD): {value}"
            ))
        })
    }

    fn resolve_range(range: Option<i32>) -> Result<i32, Status> {
        match range {
            None => Ok(DEFAULT_RANGE),
            Some(v) if v > 0 => Ok(v),
            Some(v) => Err(Status::invalid_argument(format!(
                "range harus positif (got {v})"
            ))),
        }
    }

    fn holiday_response(code: &str, date_str: &str) -> GetHakaHakiFromInvezgoResponse {
        GetHakaHakiFromInvezgoResponse {
            success: false,
            message: "hari libur".to_string(),
            code: code.to_string(),
            tahun_bulan_tanggal: date_str.to_string(),
            items: vec![],
        }
    }

    fn holiday_response_scylla() -> GetHakaHakiFromScyllaResponse {
        GetHakaHakiFromScyllaResponse {
            success: false,
            message: "hari libur".to_string(),
            items: vec![],
        }
    }

    fn minutes_now() -> u32 {
        let now = Local::now();
        now.hour() * 60 + now.minute()
    }

    /// Senin–Kamis: live 09:00–11:59 & 13:30–16:00; istirahat 12:00–13:29 → EOD cache.
    /// Jumat: live 09:00–11:29 & 14:00–16:00; istirahat 11:30–13:59 → EOD cache.
    /// Setelah 16:00 → EOD cache.
    fn require_current_day_access() -> Result<CurrentDayMode, Status> {
        let now = Local::now();
        let weekday = now.weekday();
        match weekday {
            chrono::Weekday::Sat | chrono::Weekday::Sun => {
                return Err(Status::failed_precondition(
                    "Diluar hari operasional Senin-Jumat",
                ));
            }
            _ => {}
        }

        let mins = Self::minutes_now();
        const MORNING_START: u32 = 9 * 60;
        const CLOSE: u32 = 16 * 60;
        const AFTERNOON_END: u32 = 16 * 60 + 1;

        match weekday {
            chrono::Weekday::Fri => {
                const MORNING_END: u32 = 11 * 60 + 30;
                const LUNCH_EOD_END: u32 = 14 * 60;
                const AFTERNOON_START: u32 = 14 * 60;

                if mins >= MORNING_START && mins < MORNING_END {
                    return Ok(CurrentDayMode::Live);
                }
                if mins >= MORNING_END && mins < LUNCH_EOD_END {
                    return Ok(CurrentDayMode::EodCache);
                }
                if mins >= AFTERNOON_START && mins < AFTERNOON_END {
                    return Ok(CurrentDayMode::Live);
                }
            }
            _ => {
                const MORNING_END: u32 = 12 * 60;
                const LUNCH_EOD_END: u32 = 13 * 60 + 30;
                const AFTERNOON_START: u32 = 13 * 60 + 30;

                if mins >= MORNING_START && mins < MORNING_END {
                    return Ok(CurrentDayMode::Live);
                }
                if mins >= MORNING_END && mins < LUNCH_EOD_END {
                    return Ok(CurrentDayMode::EodCache);
                }
                if mins >= AFTERNOON_START && mins < AFTERNOON_END {
                    return Ok(CurrentDayMode::Live);
                }
            }
        }

        if mins > CLOSE {
            return Ok(CurrentDayMode::EodCache);
        }

        Err(Status::failed_precondition(
            "Diluar jam operasional (Senin-Kamis 09:00-12:00 & 13:30-16:00; istirahat 12:00-13:29 cache EOD; Jumat 09:00-11:30 & 14:00-16:00; istirahat 11:30-13:59 cache EOD; setelah 16:00 cache EOD)",
        ))
    }

    async fn process_invezgo_fetch(
        session: &Session,
        code: &str,
        trade_date: NaiveDate,
        range: i32,
    ) -> Result<GetHakaHakiFromInvezgoResponse, Status> {
        let date_str = trade_date.format("%Y-%m-%d").to_string();
        let api_points = invezgo::fetch_momentum_chart(code, trade_date, range)
            .await
            .map_err(Status::internal)?;

        let mut db_rows = Vec::with_capacity(api_points.len());
        let mut items = Vec::with_capacity(api_points.len());
        for point in &api_points {
            db_rows.push(
                invezgo::api_point_to_row(code, trade_date, point).map_err(Status::internal)?,
            );
            items.push(invezgo::api_point_to_proto(point));
        }

        let saved = crate::repository::upsert_many(session, &db_rows)
            .await
            .map_err(Status::internal)?;

        Ok(GetHakaHakiFromInvezgoResponse {
            success: true,
            message: format!("{saved} baris di-upsert ke haka_haki"),
            code: code.to_string(),
            tahun_bulan_tanggal: date_str,
            items,
        })
    }
}

#[tonic::async_trait]
impl HakaHakiRpc for HakaHakiService {
    async fn get_haka_haki_from_invezgo(
        &self,
        request: Request<GetHakaHakiFromInvezgoRequest>,
    ) -> Result<Response<GetHakaHakiFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let (result, log_source, log_detail) = async {
            let req = request.into_inner();
            let code = Self::normalize_code(&req.code)?;
            let trade_date = Self::parse_trade_date(&req.tahun_bulan_tanggal)?;
            let range = Self::resolve_range(req.range)?;
            let date_str = trade_date.format("%Y-%m-%d").to_string();
            let today = Local::now().date_naive();
            let is_today = trade_date == today;

            if market_holiday::is_market_holiday_on(trade_date).await {
                return Ok((
                    Ok(Response::new(Self::holiday_response(&code, &date_str))),
                    LogSource::Api,
                    format!("{code} {date_str} range={range} holiday"),
                ));
            }

            if !is_today {
                if let Some(mut cached) =
                    crate::redis_cache::get(&code, &date_str, range).await
                {
                    cached.message = format!(
                        "{} (redis cache)",
                        cached.message.trim_end_matches(" (redis cache)")
                    );
                    let detail = format!("{code} {date_str} range={range}");
                    return Ok((
                        Ok(Response::new(cached)),
                        LogSource::Cache,
                        detail,
                    ));
                }

                let resp = Self::process_invezgo_fetch(
                    self.session.as_ref(),
                    &code,
                    trade_date,
                    range,
                )
                .await?;
                crate::redis_cache::set(&code, &date_str, range, &resp).await;
                let detail = format!("{code} {date_str} range={range}");
                return Ok((
                    Ok(Response::new(resp)),
                    LogSource::Api,
                    detail,
                ));
            }

            let mode = Self::require_current_day_access()?;

            if matches!(mode, CurrentDayMode::EodCache) {
                if let Some((mut cached, detail)) = self
                    .intraday_cache
                    .get_intraday_eod(&code, range)
                    .await
                    .map_err(Status::internal)?
                {
                    cached.message = format!(
                        "{} ({detail})",
                        cached.message.trim_end_matches(" (redis cache)")
                    );
                    return Ok((
                        Ok(Response::new(cached)),
                        LogSource::EodCache,
                        detail,
                    ));
                }

                let resp = Self::process_invezgo_fetch(
                    self.session.as_ref(),
                    &code,
                    trade_date,
                    range,
                )
                .await?;
                if let Err(e) = self
                    .intraday_cache
                    .set_intraday_eod(&code, range, &resp)
                    .await
                {
                    eprintln!("GetHakaHakiFromInvezgo set eod {code} gagal: {e}");
                }
                let detail = format!("{code} {date_str} range={range} eod MISS — GET Invezgo");
                return Ok((Ok(Response::new(resp)), LogSource::Api, detail));
            }

            let resp = Self::process_invezgo_fetch(
                self.session.as_ref(),
                &code,
                trade_date,
                range,
            )
            .await?;
            let detail = format!("{code} {date_str} range={range}");
            Ok((Ok(Response::new(resp)), LogSource::Api, detail))
        }
        .await
        .unwrap_or_else(|status: Status| (Err(status), LogSource::Api, String::new()));

        let elapsed = started.elapsed().as_millis();
        match log_source {
            LogSource::Cache | LogSource::EodCache => eprintln!(
                "\x1b[37mGetHakaHakiFromInvezgo {user_name} {elapsed}ms - HIT FROM CACHE - {log_detail}\x1b[0m"
            ),
            LogSource::Api => {
                if log_detail.is_empty() {
                    eprintln!("GetHakaHakiFromInvezgo {user_name} {elapsed}ms");
                } else {
                    eprintln!(
                        "\x1b[32mGetHakaHakiFromInvezgo {user_name} {elapsed}ms - {log_detail}\x1b[0m"
                    );
                }
            }
        }
        result
    }

    async fn get_haka_haki_from_scylla(
        &self,
        request: Request<GetHakaHakiFromScyllaRequest>,
    ) -> Result<Response<GetHakaHakiFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetHakaHakiFromScyllaResponse>, Status> = async {
            let req = request.into_inner();
            let code = match Self::normalize_code(&req.code) {
                Ok(c) => c,
                Err(status) => {
                    return Ok(Response::new(GetHakaHakiFromScyllaResponse {
                        success: false,
                        message: status.message().to_string(),
                        items: vec![],
                    }));
                }
            };
            let trade_date = match Self::parse_trade_date(&req.tahun_bulan_tanggal) {
                Ok(d) => d,
                Err(status) => {
                    return Ok(Response::new(GetHakaHakiFromScyllaResponse {
                        success: false,
                        message: status.message().to_string(),
                        items: vec![],
                    }));
                }
            };
            if market_holiday::is_market_holiday_on(trade_date).await {
                return Ok(Response::new(Self::holiday_response_scylla()));
            }

            let agg = agg_code_tahun_bulan_tanggal(&code, trade_date);
            let rows = crate::repository::find_by_agg_code_tahun_bulan_tanggal(
                self.session.as_ref(),
                &agg,
            )
            .await
            .map_err(Status::internal)?;

            let n = rows.len();
            Ok(Response::new(GetHakaHakiFromScyllaResponse {
                success: true,
                message: format!("haka_haki {agg}: {n} baris dari Scylla"),
                items: rows.into_iter().map(|r| r.into_proto()).collect(),
            }))
        }
        .await;

        eprintln!(
            "GetHakaHakiFromScylla {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
