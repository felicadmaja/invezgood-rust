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
    GetAllPortofolioFromScyllaRequest, GetAllPortofolioFromScyllaResponse,
    GetAllPortofolioFromStockbitRequest, GetAllPortofolioFromStockbitResponse,
    GetPortofolioHistoryByEmitenNameRequest, GetPortofolioHistoryByEmitenNameResponse,
    PortofolioRow,
};

fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet (contoh: ASBI)".into());
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

fn rows_to_proto(rows: Vec<Portofolio>) -> Vec<PortofolioRow> {
    rows.into_iter().map(Portofolio::into_proto).collect()
}

#[tonic::async_trait]
impl PortofolioRpc for PortofolioService {
    async fn get_all_portofolio_from_scylla(
        &self,
        request: Request<GetAllPortofolioFromScyllaRequest>,
    ) -> Result<Response<GetAllPortofolioFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows = rows_to_proto(rows);

        println!(
            "GetAllPortofolioFromScylla {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllPortofolioFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_all_portofolio_from_stockbit(
        &self,
        request: Request<GetAllPortofolioFromStockbitRequest>,
    ) -> Result<Response<GetAllPortofolioFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!(
            "GetAllPortofolioFromStockbit {username}: scrape portfolio API + upsert..."
        );

        match on_demand::scrape_portofolio_all(Arc::clone(&self.session)).await {
            Ok(n) => {
                let rows = self
                    .repo
                    .get_all()
                    .await
                    .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
                let proto_rows = rows_to_proto(rows);
                let message = format!(
                    "portofolio: scrape selesai, {n} baris di-upsert (baca {} baris)",
                    proto_rows.len()
                );
                println!(
                    "GetAllPortofolioFromStockbit {} success=true rows={} {}ms",
                    username,
                    proto_rows.len(),
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPortofolioFromStockbitResponse {
                    success: true,
                    message,
                    rows: proto_rows,
                }))
            }
            Err(e) => {
                eprintln!("GetAllPortofolioFromStockbit {username}: gagal: {e}");
                println!(
                    "GetAllPortofolioFromStockbit {} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPortofolioFromStockbitResponse {
                    success: false,
                    message: format!("scrape portofolio gagal: {e}"),
                    rows: vec![],
                }))
            }
        }
    }

    async fn get_portofolio_history_by_emiten_name(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(GetPortofolioHistoryByEmitenNameResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        println!(
            "GetPortofolioHistoryByEmitenName {username}: {kode} — order/v2/list + timpa history..."
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
                    "GetPortofolioHistoryByEmitenName {} {kode} success=true history={} {}ms",
                    username,
                    n,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetPortofolioHistoryByEmitenNameResponse {
                    success: true,
                    message,
                    row,
                }))
            }
            Err(e) => {
                eprintln!("GetPortofolioHistoryByEmitenName {username}: gagal {kode}: {e}");
                println!(
                    "GetPortofolioHistoryByEmitenName {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetPortofolioHistoryByEmitenNameResponse {
                    success: false,
                    message: format!("scrape portofolio history gagal: {e}"),
                    row: None,
                }))
            }
        }
    }
}
