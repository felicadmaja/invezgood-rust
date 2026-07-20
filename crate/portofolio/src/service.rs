use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::model::Portofolio;
use crate::portofolio_server::Portofolio as PortofolioRpc;
use crate::repository::PortofolioRepository;
use crate::{
    GetAllPortofolioRequest, GetAllPortofolioResponse,
    GetPortofolioHistoryByEmitenNameFromScyllaRequest,
    GetPortofolioHistoryByEmitenNameFromScyllaResponse,
    GetPortofolioHistoryByEmitenNameFromStockbitRequest,
    GetPortofolioHistoryByEmitenNameFromStockbitResponse, PortofolioRow,
};

fn parse_emiten_name(raw: &str) -> Result<String, Status> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(Status::invalid_argument(
            "emiten_name harus tepat 4 huruf alfabet (contoh: ASBI)",
        ));
    }
    Ok(kode)
}

pub struct PortofolioService {
    repo: PortofolioRepository,
    session: Arc<Session>,
}

impl PortofolioService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PortofolioRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl PortofolioRpc for PortofolioService {
    async fn get_all_portofolio(
        &self,
        request: Request<GetAllPortofolioRequest>,
    ) -> Result<Response<GetAllPortofolioResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows: Vec<Portofolio> = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<PortofolioRow> = rows.into_iter().map(Portofolio::into_proto).collect();

        println!(
            "GetAllPortofolio {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllPortofolioResponse {
            rows: proto_rows,
        }))
    }

    async fn get_portofolio_history_by_emiten_name_from_scylla(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromScyllaRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();
        let kode = parse_emiten_name(&req.emiten_name)?;

        let row = self
            .repo
            .find_by_emiten(&kode)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("portofolio tidak ditemukan untuk {kode}"))
            })?;

        let history_len = row.history.len();
        println!(
            "GetPortofolioHistoryByEmitenNameFromScylla {} {kode} history={} {}ms",
            username,
            history_len,
            started.elapsed().as_millis()
        );

        Ok(Response::new(
            GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                row: Some(Portofolio::into_proto(row)),
            },
        ))
    }

    async fn get_portofolio_history_by_emiten_name_from_stockbit(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromStockbitRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(status) => {
                return Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message: status.message().to_string(),
                        row: None,
                    },
                ));
            }
        };

        println!(
            "GetPortofolioHistoryByEmitenNameFromStockbit {username}: {kode} — order/v2/list + timpa history..."
        );

        match on_demand::scrape_portofolio_history_for_emiten(Arc::clone(&self.session), &kode)
            .await
        {
            Ok(n) => {
                let row = self
                    .repo
                    .find_by_emiten(&kode)
                    .await
                    .ok()
                    .flatten()
                    .map(Portofolio::into_proto);
                let message = format!(
                    "portofolio history {kode}: scrape selesai, {n} entri di-set ke history"
                );
                println!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {} {kode} success=true history={} {}ms",
                    username,
                    n,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: true,
                        message,
                        row,
                    },
                ))
            }
            Err(e) => {
                eprintln!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {username}: gagal {kode}: {e}"
                );
                println!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message: format!("scrape portofolio history gagal: {e}"),
                        row: None,
                    },
                ))
            }
        }
    }
}
