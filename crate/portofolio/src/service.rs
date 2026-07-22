use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::model::Portofolio;
use crate::portofolio_server::Portofolio as PortofolioRpc;
use crate::repository::PortofolioRepository;
use crate::{
    GetAllPortofolioFromScyllaRequest, GetAllPortofolioFromScyllaResponse,
    GetAllPortofolioFromStockbitRequest, GetAllPortofolioFromStockbitResponse, PortofolioRow,
};

/// Cooldown global antar invoke `GetAllPortofolioFromStockbit` (semua user).
const PORTFOLIO_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

static LAST_PORTFOLIO_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn portfolio_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_PORTFOLIO_SCRAPE.get_or_init(|| Mutex::new(None))
}

/// Izinkan scrape portfolio; tolak jika < 5 menit sejak invoke terakhir (global).
async fn acquire_portfolio_scrape_slot() -> Result<(), Status> {
    let mut last = portfolio_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < PORTFOLIO_SCRAPE_COOLDOWN {
            let remaining_secs = (PORTFOLIO_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 5 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
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

    /// Jam 07–17 dicek pemanggil. Rate limit 1×/5 menit + scrape (sama RPC).
    /// Returns `(baris_upsert, kode_holding)`.
    pub async fn scrape_from_stockbit_if_allowed(
        &self,
    ) -> Result<(usize, Vec<String>), Status> {
        acquire_portfolio_scrape_slot().await?;
        on_demand::scrape_portofolio_all(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(e))
    }

    /// Kode holding saat ini di Scylla `portofolio` (untuk batch history).
    pub async fn list_holding_codes(&self) -> Result<Vec<String>, Status> {
        let rows = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| r.emiten_name.trim().to_ascii_uppercase())
            .filter(|c| !c.is_empty())
            .collect())
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

        require_stockbit_scrape_hours()?;
        acquire_portfolio_scrape_slot().await?;

        println!(
            "GetAllPortofolioFromStockbit {username}: scrape portfolio API + upsert..."
        );

        match on_demand::scrape_portofolio_all(Arc::clone(&self.session)).await {
            Ok((n, _)) => {
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
}
