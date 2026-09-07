use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use user::{extract_bearer_token, validate_session, SessionStore};

use crate::download::STREAM_CHUNK_BYTES;
use crate::pb::xlbr_laporan_keuangan_server::XlbrLaporanKeuangan;
use crate::pb::{
    DownloadInlineXbrlRequest, DownloadInlineXbrlResponse, GetCatatanByCodeRequest,
    GetCatatanByCodeResponse, GetXlbrChartByCodeRequest, GetXlbrChartByCodeResponse,
    ScrapZipFromBeiRequest, UploadZipChunk, UploadZipResponse, UpsertCatatanByCodeRequest,
    UpsertCatatanByCodeResponse, XlbrChartPoint,
};
use crate::repository;

const CHART_LIMIT: i32 = 20;

type DownloadInlineXbrlStream =
    Pin<Box<dyn Stream<Item = Result<DownloadInlineXbrlResponse, Status>> + Send>>;

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

    async fn require_auth_token(&self, token: &str) -> Result<String, Status> {
        let auth = validate_session(&self.auth_sessions, token)
            .await
            .map_err(Status::unauthenticated)?;
        Ok(auth.nama)
    }
}

#[tonic::async_trait]
impl XlbrLaporanKeuangan for XlbrLaporanKeuanganService {
    type DownloadInlineXBRLStream = DownloadInlineXbrlStream;

    async fn upload_zip(
        &self,
        request: Request<Streaming<UploadZipChunk>>,
    ) -> Result<Response<UploadZipResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let user_name = self.require_auth_token(&token).await?;

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
        let user_name = self.require_auth_token(&token).await?;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        if code.is_empty() {
            eprintln!(
                "ScrapZipFromBei {user_name} {}ms",
                started.elapsed().as_millis()
            );
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let result: Result<Response<UploadZipResponse>, Status> = async {
            let outcome = crate::bei_scraper::enqueue_scrap_job(self.session.clone(), &code)
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

    async fn download_inline_xbrl(
        &self,
        request: Request<DownloadInlineXbrlRequest>,
    ) -> Result<Response<Self::DownloadInlineXBRLStream>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let user_name = self.require_auth_token(&token).await?;
        let req = request.into_inner();
        let code = req.code.trim().to_ascii_uppercase();
        let tahun_quarter: Vec<String> = req
            .tahun_quarter
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect();

        if code.is_empty() {
            eprintln!(
                "DownloadInlineXBRL {user_name} {}ms",
                started.elapsed().as_millis()
            );
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let code_bg = code.clone();
        let tahun_quarter_bg = tahun_quarter.clone();
        let user_name_bg = user_name.clone();

        tokio::spawn(async move {
            let result =
                crate::download::resolve_emiten_download(&code_bg, &tahun_quarter_bg).await;
            match result {
                Ok((bytes, message)) => {
                    for chunk in bytes.chunks(STREAM_CHUNK_BYTES) {
                        if tx
                            .send(Ok(DownloadInlineXbrlResponse {
                                success: false,
                                message: String::new(),
                                data: chunk.to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            eprintln!(
                                "DownloadInlineXBRL {user_name_bg} {}ms",
                                started.elapsed().as_millis()
                            );
                            return;
                        }
                    }
                    let _ = tx
                        .send(Ok(DownloadInlineXbrlResponse {
                            success: true,
                            message,
                            data: Vec::new(),
                        }))
                        .await;
                }
                Err(e) => {
                    let _ = tx
                        .send(Ok(DownloadInlineXbrlResponse {
                            success: false,
                            message: e,
                            data: Vec::new(),
                        }))
                        .await;
                }
            }

            eprintln!(
                "DownloadInlineXBRL {user_name_bg} {}ms",
                started.elapsed().as_millis()
            );
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as DownloadInlineXbrlStream,
        ))
    }

    async fn get_catatan_by_code(
        &self,
        request: Request<GetCatatanByCodeRequest>,
    ) -> Result<Response<GetCatatanByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let user_name = self.require_auth_token(&token).await?;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetCatatanByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let catatan = repository::get_catatan_by_code(self.session.as_ref(), &code)
                .await
                .map_err(|e| {
                    if e.contains("tidak ditemukan") {
                        Status::not_found(e)
                    } else {
                        Status::internal(e)
                    }
                })?;

            Ok(Response::new(GetCatatanByCodeResponse {
                success: true,
                message: format!("{} entri catatan", catatan.len()),
                code,
                catatan,
            }))
        }
        .await;

        eprintln!(
            "GetCatatanByCode {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn upsert_catatan_by_code(
        &self,
        request: Request<UpsertCatatanByCodeRequest>,
    ) -> Result<Response<UpsertCatatanByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let user_name = self.require_auth_token(&token).await?;
        let req = request.into_inner();
        let code = req.code.trim().to_ascii_uppercase();

        let result: Result<Response<UpsertCatatanByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let updated = repository::upsert_catatan_by_code(
                self.session.as_ref(),
                &code,
                req.catatan,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpsertCatatanByCodeResponse {
                success: true,
                message: format!("catatan {code} diupdate pada {updated} baris"),
                code,
            }))
        }
        .await;

        eprintln!(
            "UpsertCatatanByCode {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn get_chart_by_code(
        &self,
        request: Request<GetXlbrChartByCodeRequest>,
    ) -> Result<Response<GetXlbrChartByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let token = extract_bearer_token(&request)?;
        let user_name = self.require_auth_token(&token).await?;
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
                    interest_paid: r.interest_paid,
                    tax_paid: r.tax_paid,
                    presentation_currency: r.presentation_currency,
                    unit_scale: r.unit_scale,
                    catatan: r.catatan.clone().unwrap_or_default(),
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
