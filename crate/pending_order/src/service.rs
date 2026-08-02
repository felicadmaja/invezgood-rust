use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{
    extract_bearer_token, require_stockbit_scrape_hours, validate_session, AuthSession,
    SessionStore,
};
use worker_scrapping::on_demand;

use crate::model::PendingOrderRow as DbPendingOrderRow;
use crate::pb::pending_order_server::PendingOrder;
use crate::pb::{
    CreateBuyLimitOrderRequest, CreateBuyLimitOrderResponse, ExpiryPendingOrder,
    GetAllPendingOrderFromScyllaRequest, GetAllPendingOrderFromScyllaResponse,
    GetAllPendingOrderFromStockbitRequest, GetAllPendingOrderFromStockbitResponse,
    GetPendingOrderFromScyllaByEmitenNameRequest, GetPendingOrderFromScyllaByEmitenNameResponse,
    PendingOrderRow,
};

const PENDING_ORDER_SCRAPE_COOLDOWN: Duration = Duration::from_secs(3 * 60);
const BUY_LIMIT_COOLDOWN: Duration = Duration::from_secs(30);

static LAST_PENDING_ORDER_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static LAST_BUY_LIMIT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn pending_order_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_PENDING_ORDER_SCRAPE.get_or_init(|| Mutex::new(None))
}

fn buy_limit_gate() -> &'static Mutex<Option<Instant>> {
    LAST_BUY_LIMIT.get_or_init(|| Mutex::new(None))
}

async fn acquire_pending_order_scrape_slot() -> Result<(), Status> {
    let mut last = pending_order_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < PENDING_ORDER_SCRAPE_COOLDOWN {
            let remaining_secs = (PENDING_ORDER_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 3 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

async fn acquire_buy_limit_slot() -> Result<(), Status> {
    let mut last = buy_limit_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < BUY_LIMIT_COOLDOWN {
            let remaining_secs = (BUY_LIMIT_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 30 detik untuk semua user. Tunggu {remaining_secs} detik lagi"
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

#[derive(Clone)]
pub struct PendingOrderService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl PendingOrderService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
            auth_sessions,
        }
    }

    pub async fn scrape_from_stockbit_if_allowed(&self) -> Result<usize, Status> {
        acquire_pending_order_scrape_slot().await?;
        on_demand::scrape_pending_order_all(Arc::clone(&self.session))
            .await
            .map_err(Status::internal)
    }

    pub async fn scrape_from_stockbit_if_allowed_background(&self) -> Result<usize, Status> {
        acquire_pending_order_scrape_slot().await?;
        on_demand::scrape_pending_order_all_background(Arc::clone(&self.session))
            .await
            .map_err(Status::internal)
    }

    async fn require_admin<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))?;
        if auth.role.trim().eq_ignore_ascii_case("admin") {
            Ok(auth)
        } else {
            Err(Status::permission_denied("Harus admin !"))
        }
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }

    fn normalize_emiten_name(raw: &str) -> Result<String, Status> {
        let name = raw.trim().to_ascii_uppercase();
        if name.len() != 4 || !name.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "emiten_name tidak valid ({raw}); wajib tepat 4 huruf alphabet"
            )));
        }
        Ok(name)
    }

    fn row_to_proto(row: DbPendingOrderRow) -> PendingOrderRow {
        PendingOrderRow {
            order_id: row.order_id,
            emiten_name: row.emiten_name,
            status: row.status,
            message: row.message,
            side: row.side,
            time_open: row
                .time_open
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
            lot_open: row.lot_open,
            lot_done: row.lot_done,
            price_order: row.price_order,
            amount_open: row.amount_open,
            amount_match: row.amount_match,
            amount_match_total: row.amount_match_total,
            is_gtc: row.is_gtc,
            updated_at: row
                .updated_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl PendingOrder for PendingOrderService {
    async fn get_all_pending_order_from_scylla(
        &self,
        request: Request<GetAllPendingOrderFromScyllaRequest>,
    ) -> Result<Response<GetAllPendingOrderFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllPendingOrderFromScyllaResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = crate::repository::find_all(self.session.as_ref())
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetAllPendingOrderFromScyllaResponse {
                rows: rows.into_iter().map(Self::row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllPendingOrderFromScylla", &user_name, started);
        result
    }

    async fn get_pending_order_from_scylla_by_emiten_name(
        &self,
        request: Request<GetPendingOrderFromScyllaByEmitenNameRequest>,
    ) -> Result<Response<GetPendingOrderFromScyllaByEmitenNameResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetPendingOrderFromScyllaByEmitenNameResponse>, Status> =
            async {
                let emiten_name =
                    Self::normalize_emiten_name(&request.into_inner().emiten_name)?;
                let rows =
                    crate::repository::find_by_emiten_name(self.session.as_ref(), &emiten_name)
                        .await
                        .map_err(Status::internal)?;

                Ok(Response::new(GetPendingOrderFromScyllaByEmitenNameResponse {
                    rows: rows.into_iter().map(Self::row_to_proto).collect(),
                }))
            }
            .await;

        Self::log_rpc_debug("GetPendingOrderFromScyllaByEmitenName", &user_name, started);
        result
    }

    async fn get_all_pending_order_from_stockbit(
        &self,
        request: Request<GetAllPendingOrderFromStockbitRequest>,
    ) -> Result<Response<GetAllPendingOrderFromStockbitResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllPendingOrderFromStockbitResponse>, Status> = async {
            let _inner = request.into_inner();
            require_stockbit_scrape_hours()?;
            acquire_pending_order_scrape_slot().await?;

            match on_demand::scrape_pending_order_all(Arc::clone(&self.session)).await {
                Ok(n) => {
                    let rows = crate::repository::find_all(self.session.as_ref())
                        .await
                        .map_err(Status::internal)?;
                    let proto_rows: Vec<PendingOrderRow> =
                        rows.into_iter().map(Self::row_to_proto).collect();
                    Ok(Response::new(GetAllPendingOrderFromStockbitResponse {
                        success: true,
                        message: format!(
                            "pending_order: scrape selesai, {n} baris di-upsert (baca {} baris)",
                            proto_rows.len()
                        ),
                        rows: proto_rows,
                    }))
                }
                Err(e) => Ok(Response::new(GetAllPendingOrderFromStockbitResponse {
                    success: false,
                    message: format!("scrape pending_order gagal: {e}"),
                    rows: vec![],
                })),
            }
        }
        .await;

        Self::log_rpc_debug("GetAllPendingOrderFromStockbit", &user_name, started);
        result
    }

    async fn create_buy_limit_order(
        &self,
        request: Request<CreateBuyLimitOrderRequest>,
    ) -> Result<Response<CreateBuyLimitOrderResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;
        let req = request.into_inner();

        let emiten_name = Self::normalize_emiten_name(&req.emiten_name)?;
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
        let expiry =
            ExpiryPendingOrder::try_from(req.expiry).unwrap_or(ExpiryPendingOrder::Unspecified);
        let expiry_dom = expiry_dom_value(expiry)?;

        acquire_buy_limit_slot().await?;

        let result: Result<Response<CreateBuyLimitOrderResponse>, Status> = async {
            match on_demand::create_buy_limit_order(
                emiten_name.clone(),
                req.buylimit_price,
                req.lot,
                expiry_dom.to_string(),
            )
            .await
            {
                Ok(()) => Ok(Response::new(CreateBuyLimitOrderResponse {
                    success: true,
                    message: "Order limit buy berhasil dibuat".to_string(),
                })),
                Err(e) => {
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
        .await;

        Self::log_rpc_debug("CreateBuyLimitOrder", &user_name, started);
        result
    }
}
