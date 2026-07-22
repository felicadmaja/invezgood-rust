use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::model::PendingOrder;
use crate::pending_order_server::PendingOrder as PendingOrderRpc;
use crate::repository::PendingOrderRepository;
use crate::{
    GetAllPendingOrderFromScyllaRequest, GetAllPendingOrderFromScyllaResponse,
    GetAllPendingOrderFromStockbitRequest, GetAllPendingOrderFromStockbitResponse, PendingOrderRow,
};

/// Cooldown global antar invoke `GetAllPendingOrderFromStockbit` (semua user).
const PENDING_ORDER_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

static LAST_PENDING_ORDER_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn pending_order_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_PENDING_ORDER_SCRAPE.get_or_init(|| Mutex::new(None))
}

/// Izinkan scrape pending order; tolak jika < 5 menit sejak invoke terakhir (global).
async fn acquire_pending_order_scrape_slot() -> Result<(), Status> {
    let mut last = pending_order_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < PENDING_ORDER_SCRAPE_COOLDOWN {
            let remaining_secs = (PENDING_ORDER_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 5 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

pub struct PendingOrderService {
    repo: PendingOrderRepository,
    session: Arc<Session>,
}

impl PendingOrderService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PendingOrderRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }

    /// Rate limit 1×/5 menit + scrape pending order (sama RPC). Jam 07–17 dicek pemanggil.
    /// Dipakai juga auto `IsStockbitReady` — jatah rate limit terpakai bersama user RPC.
    pub async fn scrape_from_stockbit_if_allowed(&self) -> Result<usize, Status> {
        acquire_pending_order_scrape_slot().await?;
        on_demand::scrape_pending_order_all(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(e))
    }
}

fn rows_to_proto(rows: Vec<PendingOrder>) -> Vec<PendingOrderRow> {
    rows.into_iter().map(PendingOrder::into_proto).collect()
}

#[tonic::async_trait]
impl PendingOrderRpc for PendingOrderService {
    async fn get_all_pending_order_from_scylla(
        &self,
        request: Request<GetAllPendingOrderFromScyllaRequest>,
    ) -> Result<Response<GetAllPendingOrderFromScyllaResponse>, Status> {
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
            "GetAllPendingOrderFromScylla {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllPendingOrderFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_all_pending_order_from_stockbit(
        &self,
        request: Request<GetAllPendingOrderFromStockbitRequest>,
    ) -> Result<Response<GetAllPendingOrderFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        require_stockbit_scrape_hours()?;
        acquire_pending_order_scrape_slot().await?;

        println!(
            "GetAllPendingOrderFromStockbit {username}: scrape order/v2/list + upsert..."
        );

        match on_demand::scrape_pending_order_all(Arc::clone(&self.session)).await {
            Ok(n) => {
                let rows = self
                    .repo
                    .get_all()
                    .await
                    .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
                let proto_rows = rows_to_proto(rows);
                let message = format!(
                    "pending_order: scrape selesai, {n} baris di-upsert (baca {} baris)",
                    proto_rows.len()
                );
                println!(
                    "GetAllPendingOrderFromStockbit {} success=true rows={} {}ms",
                    username,
                    proto_rows.len(),
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPendingOrderFromStockbitResponse {
                    success: true,
                    message,
                    rows: proto_rows,
                }))
            }
            Err(e) => {
                eprintln!("GetAllPendingOrderFromStockbit {username}: gagal: {e}");
                println!(
                    "GetAllPendingOrderFromStockbit {} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetAllPendingOrderFromStockbitResponse {
                    success: false,
                    message: format!("scrape pending_order gagal: {e}"),
                    rows: vec![],
                }))
            }
        }
    }
}
