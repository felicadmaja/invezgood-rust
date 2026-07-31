use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::model::PortofolioEquity;
use crate::portofolio_equity_server::PortofolioEquity as PortofolioEquityRpc;
use crate::repository::PortofolioEquityRepository;
use crate::{
    GetAllPortofolioEquityFromScyllaRequest, GetAllPortofolioEquityFromScyllaResponse,
    GetAllPortofolioEquityFromStockbitRequest, GetAllPortofolioEquityFromStockbitResponse,
    PortofolioEquityRow,
};

/// Cooldown global antar invoke `GetAllPortofolioEquityFromStockbit` (semua user).
const EQUITY_SCRAPE_COOLDOWN: Duration = Duration::from_secs(3 * 60);

static LAST_EQUITY_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn equity_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_EQUITY_SCRAPE.get_or_init(|| Mutex::new(None))
}

async fn acquire_equity_scrape_slot() -> Result<(), Status> {
    let mut last = equity_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < EQUITY_SCRAPE_COOLDOWN {
            let remaining_secs = (EQUITY_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 3 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

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

    /// Rate limit 1×/3 menit + scrape (sama RPC). Jam 07–17 dicek pemanggil.
    /// Dipakai juga auto `IsStockbitReady` — jatah rate limit terpakai bersama user RPC.
    pub async fn scrape_from_stockbit_if_allowed(&self) -> Result<usize, Status> {
        acquire_equity_scrape_slot().await?;
        on_demand::scrape_portofolio_equity(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(e))
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

        require_stockbit_scrape_hours()?;
        acquire_equity_scrape_slot().await?;

        println!(
            "GetAllPortofolioEquityFromStockbit {username}: scrape portfolio/v2/list summary..."
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
