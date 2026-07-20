use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::PendingOrder;
use crate::pending_order_server::PendingOrder as PendingOrderRpc;
use crate::repository::PendingOrderRepository;
use crate::{
    GetAllOpenPendingOrderRequest, GetAllOpenPendingOrderResponse, PendingOrderRow,
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

        let proto_rows: Vec<PendingOrderRow> =
            rows.into_iter().map(PendingOrder::into_proto).collect();

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
}
