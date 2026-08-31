use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status, Streaming};
use user::{extract_bearer_token, validate_session, SessionStore};

use crate::pb::xlbr_laporan_keuangan_server::XlbrLaporanKeuangan;
use crate::pb::{
    GetXlbrChartByCodeRequest, GetXlbrChartByCodeResponse, ScrapZipFromBeiRequest,
    UploadZipChunk, UploadZipResponse, XlbrChartPoint,
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

    fn map_upload_error(e: String) -> Status {
        Status::invalid_argument(e)
    }
}

#[tonic::async_trait]
impl XlbrLaporanKeuangan for XlbrLaporanKeuanganService {
    async fn upload_zip(
        &self,
        request: Request<Streaming<UploadZipChunk>>,
    ) -> Result<Response<UploadZipResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(Status::unauthenticated)?;
        let user_name = auth.nama;

        let result: Result<Response<UploadZipResponse>, Status> = async {
            let mut stream = request.into_inner();
            let mut zip_bytes = Vec::new();

            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|e| Status::internal(format!("stream chunk: {e}")))?
            {
                if chunk.data.is_empty() {
                    continue;
                }
                zip_bytes.extend_from_slice(&chunk.data);
            }

            let row = crate::upload_from_zip_bytes(self.session.clone(), &zip_bytes)
                .await
                .map_err(Self::map_upload_error)?;

            Ok(Response::new(UploadZipResponse {
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
            "UploadZip {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn scrap_zip_from_bei(
        &self,
        request: Request<ScrapZipFromBeiRequest>,
    ) -> Result<Response<UploadZipResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(Status::unauthenticated)?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<UploadZipResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let outcome = crate::bei_scraper::scrap_and_upload(self.session.clone(), &code)
                .await
                .map_err(Status::internal)?;

            let last = outcome.last_row.as_ref();
            Ok(Response::new(UploadZipResponse {
                success: outcome.uploaded > 0,
                message: format!(
                    "scrap {}: uploaded {} skipped {} failed {}",
                    code, outcome.uploaded, outcome.skipped, outcome.failed
                ),
                code: last.map(|r| r.code.clone()).unwrap_or(code),
                fiscal_year: last.map(|r| r.fiscal_year).unwrap_or(0),
                quarter: last
                    .map(|r| r.quarter.clone())
                    .unwrap_or_default(),
            }))
        }
        .await;

        eprintln!(
            "ScrapZipFromBei {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn get_chart_by_code(
        &self,
        request: Request<GetXlbrChartByCodeRequest>,
    ) -> Result<Response<GetXlbrChartByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let user_name = "anonymous";
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
