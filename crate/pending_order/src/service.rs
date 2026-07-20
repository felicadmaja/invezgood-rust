use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::model::PendingOrder;
use crate::pending_order_server::PendingOrder as PendingOrderRpc;
use crate::repository::PendingOrderRepository;
use crate::{
    GetAllPendingOrderFromScyllaRequest, GetAllPendingOrderFromScyllaResponse,
    GetAllPendingOrderFromStockbitRequest, GetAllPendingOrderFromStockbitResponse, PendingOrderRow,
};

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
