use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::PendingOrder;
use crate::pending_order_server::PendingOrder as PendingOrderRpc;
use crate::repository::PendingOrderRepository;
use crate::{
    GetAllMatchPendingOrderRequest, GetAllMatchPendingOrderResponse,
    GetAllOpenPendingOrderRequest, GetAllOpenPendingOrderResponse,
    GetAllRejectedPendingOrderRequest, GetAllRejectedPendingOrderResponse, PendingOrderRow,
};

pub struct PendingOrderService {
    repo: PendingOrderRepository,
}

impl PendingOrderService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: PendingOrderRepository::new(session),
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
    async fn get_all_open_pending_order(
        &self,
        request: Request<GetAllOpenPendingOrderRequest>,
    ) -> Result<Response<GetAllOpenPendingOrderResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all_open()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows = rows_to_proto(rows);
        println!(
            "GetAllOpenPendingOrder {} status=OPEN rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllOpenPendingOrderResponse {
            rows: proto_rows,
        }))
    }

    async fn get_all_match_pending_order(
        &self,
        request: Request<GetAllMatchPendingOrderRequest>,
    ) -> Result<Response<GetAllMatchPendingOrderResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all_match()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows = rows_to_proto(rows);
        println!(
            "GetAllMatchPendingOrder {} status=MATCH rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllMatchPendingOrderResponse {
            rows: proto_rows,
        }))
    }

    async fn get_all_rejected_pending_order(
        &self,
        request: Request<GetAllRejectedPendingOrderRequest>,
    ) -> Result<Response<GetAllRejectedPendingOrderResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all_rejected()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows = rows_to_proto(rows);
        println!(
            "GetAllRejectedPendingOrder {} status=REJECTED rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllRejectedPendingOrderResponse {
            rows: proto_rows,
        }))
    }
}
