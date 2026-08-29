use std::sync::Arc;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::pb::xlbr_laporan_keuangan_server::XlbrLaporanKeuangan;
use crate::pb::{
    GetXlbrChartByCodeRequest, GetXlbrChartByCodeResponse, UploadXlbrFromUrlRequest,
    UploadXlbrFromUrlResponse, XlbrChartPoint,
};
use crate::repository;

const CHART_LIMIT: i32 = 20;

pub struct XlbrLaporanKeuanganService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl XlbrLaporanKeuanganService {
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
            .map_err(|e| Status::unauthenticated(e))
    }
}

#[tonic::async_trait]
impl XlbrLaporanKeuangan for XlbrLaporanKeuanganService {
    async fn upload_from_url(
        &self,
        request: Request<UploadXlbrFromUrlRequest>,
    ) -> Result<Response<UploadXlbrFromUrlResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let url = request.into_inner().url;

        let result: Result<Response<UploadXlbrFromUrlResponse>, Status> = async {
            let row = crate::upload_from_url(self.session.clone(), &url)
                .await
                .map_err(|e| {
                    if e.contains("membutuhkan") || e.contains("serial") || e.contains("sudah ada") {
                        Status::failed_precondition(e)
                    } else {
                        Status::invalid_argument(e)
                    }
                })?;

            Ok(Response::new(UploadXlbrFromUrlResponse {
                success: true,
                message: format!(
                    "upload {} {} {} standalone CFO={:.0} net_income={:.0}",
                    row.code, row.fiscal_year, row.quarter, row.cash_from_operation, row.net_income
                ),
                code: row.code,
                fiscal_year: row.fiscal_year,
                quarter: row.quarter,
            }))
        }
        .await;

        eprintln!(
            "UploadFromUrl {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn get_chart_by_code(
        &self,
        request: Request<GetXlbrChartByCodeRequest>,
    ) -> Result<Response<GetXlbrChartByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetXlbrChartByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let rows = repository::list_chart(self.session.as_ref(), &code, CHART_LIMIT)
                .await
                .map_err(Status::internal)?;

            let points: Vec<XlbrChartPoint> = rows
                .into_iter()
                .map(|r| XlbrChartPoint {
                    fiscal_year: r.fiscal_year,
                    quarter: r.quarter,
                    period_end: r.period_end.timestamp(),
                    cash_from_operation: r.cash_from_operation,
                    cash_from_investment: r.cash_from_investment,
                    cash_from_financing: r.cash_from_financing,
                    capital_expenditure: r.capital_expenditure,
                    free_cash_flow: r.free_cash_flow,
                    net_income: r.net_income,
                    presentation_currency: r.presentation_currency,
                    unit_scale: r.unit_scale,
                })
                .collect();

            Ok(Response::new(GetXlbrChartByCodeResponse {
                success: true,
                message: format!("{} titik grafik", points.len()),
                code,
                points,
            }))
        }
        .await;

        eprintln!(
            "GetChartByCode {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
