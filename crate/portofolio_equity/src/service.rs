use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::model::PortofolioEquity;
use crate::portofolio_equity_server::PortofolioEquity as PortofolioEquityRpc;
use crate::repository::PortofolioEquityRepository;
use crate::{
    GetAllPortofolioEquityFromScyllaRequest, GetAllPortofolioEquityFromScyllaResponse,
    GetAllPortofolioEquityFromStockbitRequest, GetAllPortofolioEquityFromStockbitResponse,
    PortofolioEquityRow,
};

pub struct PortofolioEquityService {
    repo: PortofolioEquityRepository,
    session: Arc<Session>,
}

impl PortofolioEquityService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PortofolioEquityRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

fn rows_to_proto(rows: Vec<PortofolioEquity>) -> Vec<PortofolioEquityRow> {
    rows.into_iter().map(PortofolioEquity::into_proto).collect()
}

#[tonic::async_trait]
impl PortofolioEquityRpc for PortofolioEquityService {
    async fn get_all_portofolio_equity_from_scylla(
        &self,
        request: Request<GetAllPortofolioEquityFromScyllaRequest>,
    ) -> Result<Response<GetAllPortofolioEquityFromScyllaResponse>, Status> {
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
            "GetAllPortofolioEquityFromScylla {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllPortofolioEquityFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_all_portofolio_equity_from_stockbit(
        &self,
        request: Request<GetAllPortofolioEquityFromStockbitRequest>,
    ) -> Result<Response<GetAllPortofolioEquityFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!(
            "GetAllPortofolioEquityFromStockbit {username}: scrape DOM header equity..."
        );

        match on_demand::scrape_portofolio_equity(Arc::clone(&self.session)).await {
            Ok(n) => {
                let rows = self
                    .repo
                    .get_all()
                    .await
                    .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
                let proto_rows = rows_to_proto(rows);
                let message = format!(
                    "portofolio_equity: scrape selesai, {n} baris di-upsert (baca {} baris)",
                    proto_rows.len()
                );
                println!(
                    "GetAllPortofolioEquityFromStockbit {} success=true rows={} {}ms",
                    username,
                    proto_rows.len(),
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPortofolioEquityFromStockbitResponse {
                    success: true,
                    message,
                    rows: proto_rows,
                }))
            }
            Err(e) => {
                eprintln!("GetAllPortofolioEquityFromStockbit {username}: gagal: {e}");
                println!(
                    "GetAllPortofolioEquityFromStockbit {} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPortofolioEquityFromStockbitResponse {
                    success: false,
                    message: format!("scrape portofolio_equity gagal: {e}"),
                    rows: vec![],
                }))
            }
        }
    }
}
