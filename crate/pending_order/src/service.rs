use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{require_admin, require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::model::PendingOrder;
use crate::pending_order_server::PendingOrder as PendingOrderRpc;
use crate::repository::PendingOrderRepository;
use crate::{
    CreateBuyLimitOrderRequest, CreateBuyLimitOrderResponse, ExpiryPendingOrder,
    GetAllPendingOrderFromScyllaRequest, GetAllPendingOrderFromScyllaResponse,
    GetAllPendingOrderFromStockbitRequest, GetAllPendingOrderFromStockbitResponse,
    GetPendingOrderFromScyllaByEmitenNameRequest, GetPendingOrderFromScyllaByEmitenNameResponse,
    PendingOrderRow,
};

fn normalize_emiten_name(raw: &str) -> Result<String, String> {
    let name = raw.trim().to_ascii_uppercase();
    if name.len() != 4 || !name.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!(
            "emiten_name tidak valid ({raw}); wajib tepat 4 huruf alphabet"
        ));
    }
    Ok(name)
}

/// Cooldown global antar invoke `GetAllPendingOrderFromStockbit` (semua user).
const PENDING_ORDER_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// Cooldown global antar invoke `CreateBuyLimitOrder` (semua user).
const BUY_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);

static LAST_PENDING_ORDER_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static LAST_BUY_LIMIT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn pending_order_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_PENDING_ORDER_SCRAPE.get_or_init(|| Mutex::new(None))
}

fn buy_limit_gate() -> &'static Mutex<Option<Instant>> {
    LAST_BUY_LIMIT.get_or_init(|| Mutex::new(None))
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

/// Izinkan CreateBuyLimitOrder; tolak jika < 60 detik sejak invoke terakhir (global).
async fn acquire_buy_limit_slot() -> Result<(), Status> {
    let mut last = buy_limit_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < BUY_LIMIT_COOLDOWN {
            let remaining_secs = (BUY_LIMIT_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 60 detik untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

fn expiry_dom_value(expiry: ExpiryPendingOrder) -> Result<&'static str, Status> {
    match expiry {
        ExpiryPendingOrder::Gfd => Ok("0"),
        ExpiryPendingOrder::Gtc => Ok("1"),
        ExpiryPendingOrder::Unspecified => Err(Status::invalid_argument(
            "ExpiryPendingOrder harus GFD atau GTC",
        )),
    }
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

    async fn get_pending_order_from_scylla_by_emiten_name(
        &self,
        request: Request<GetPendingOrderFromScyllaByEmitenNameRequest>,
    ) -> Result<Response<GetPendingOrderFromScyllaByEmitenNameResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name =
            normalize_emiten_name(&req.emiten_name).map_err(Status::invalid_argument)?;

        let rows = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows = rows_to_proto(rows);
        println!(
            "GetPendingOrderFromScyllaByEmitenName {} {emiten_name} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetPendingOrderFromScyllaByEmitenNameResponse {
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

    async fn create_buy_limit_order(
        &self,
        request: Request<CreateBuyLimitOrderRequest>,
    ) -> Result<Response<CreateBuyLimitOrderResponse>, Status> {
        let started = Instant::now();
        let claims = require_admin(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name =
            normalize_emiten_name(&req.emiten_name).map_err(Status::invalid_argument)?;
        if req.current_price <= 0 {
            return Err(Status::invalid_argument(
                "current_price harus integer positif (> 0)",
            ));
        }
        if req.buylimit_price <= 0 {
            return Err(Status::invalid_argument(
                "buylimit_price harus integer positif (> 0)",
            ));
        }
        if req.buylimit_price >= req.current_price {
            return Err(Status::invalid_argument(format!(
                "buylimit_price ({}) harus lebih kecil dari current_price ({})",
                req.buylimit_price, req.current_price
            )));
        }
        if req.lot <= 0 {
            return Err(Status::invalid_argument("lot harus integer positif (> 0)"));
        }
        let expiry = ExpiryPendingOrder::try_from(req.expiry).unwrap_or(ExpiryPendingOrder::Unspecified);
        let expiry_dom = expiry_dom_value(expiry)?;

        acquire_buy_limit_slot().await?;

        println!(
            "CreateBuyLimitOrder {username}: {emiten_name} buylimit={} current={} lot={} expiry={:?}...",
            req.buylimit_price, req.current_price, req.lot, expiry
        );

        match on_demand::create_buy_limit_order(
            emiten_name.clone(),
            req.buylimit_price,
            req.lot,
            expiry_dom.to_string(),
        )
        .await
        {
            Ok(()) => {
                println!(
                    "CreateBuyLimitOrder {} {emiten_name} success=true {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(CreateBuyLimitOrderResponse {
                    success: true,
                    message: "Order limit buy berhasil dibuat".to_string(),
                }))
            }
            Err(e) => {
                eprintln!("CreateBuyLimitOrder {username} {emiten_name}: gagal: {e}");
                println!(
                    "CreateBuyLimitOrder {} {emiten_name} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                let message = if e.starts_with("Balance kurang") {
                    e
                } else {
                    "Order limit buy gagal dibuat".to_string()
                };
                Ok(Response::new(CreateBuyLimitOrderResponse {
                    success: false,
                    message,
                }))
            }
        }
    }
}
