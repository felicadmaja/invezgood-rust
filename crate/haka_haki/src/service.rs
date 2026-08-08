use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::invezgo::{self, ApiHakaHakiPoint};
use crate::model::agg_code_tahun_bulan_tanggal;
use crate::pb::haka_haki_server::HakaHaki as HakaHakiRpc;
use crate::pb::{
    GetHakaHakiFromInvezgoRequest, GetHakaHakiFromInvezgoResponse,
    GetHakaHakiFromScyllaRequest, GetHakaHakiFromScyllaResponse,
};

const DEFAULT_RANGE: i32 = 5;

pub struct HakaHakiService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl HakaHakiService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
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

    fn ensure_weekday_market(trade_date: NaiveDate) -> Result<(), Status> {
        match trade_date.weekday() {
            chrono::Weekday::Sat | chrono::Weekday::Sun => {
                Err(Status::failed_precondition("Hari sabtu/minggu market libur"))
            }
            _ => Ok(()),
        }
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

        let result: Result<Response<GetHakaHakiFromInvezgoResponse>, Status> = async {
            let req = request.into_inner();
            let code = Self::normalize_code(&req.code)?;
            let trade_date = Self::parse_trade_date(&req.tahun_bulan_tanggal)?;
            Self::ensure_weekday_market(trade_date)?;
            let range = Self::resolve_range(req.range)?;
            let date_str = trade_date.format("%Y-%m-%d").to_string();

            let api_points: Vec<ApiHakaHakiPoint> =
                invezgo::fetch_momentum_chart(&code, trade_date, range)
                    .await
                    .map_err(Status::internal)?;

            let mut db_rows = Vec::with_capacity(api_points.len());
            let mut items = Vec::with_capacity(api_points.len());
            for point in &api_points {
                db_rows.push(
                    invezgo::api_point_to_row(&code, trade_date, point)
                        .map_err(Status::internal)?,
                );
                items.push(invezgo::api_point_to_proto(point));
            }

            let saved = crate::repository::upsert_many(self.session.as_ref(), &db_rows)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetHakaHakiFromInvezgoResponse {
                success: true,
                message: format!("{saved} baris di-upsert ke haka_haki"),
                code,
                tahun_bulan_tanggal: date_str,
                items,
            }))
        }
        .await;

        eprintln!(
            "GetHakaHakiFromInvezgo {user_name} {}ms",
            started.elapsed().as_millis()
        );
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
            Self::ensure_weekday_market(trade_date)?;

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
