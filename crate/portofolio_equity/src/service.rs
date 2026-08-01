use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::PortofolioEquity;
use crate::portofolio_equity_server::PortofolioEquity as PortofolioEquityRpc;
use crate::repository::PortofolioEquityRepository;
use crate::{
    GetAllPortofolioEquityFromScyllaRequest, GetAllPortofolioEquityFromScyllaResponse,
    PortofolioEquityRow,
};

pub struct PortofolioEquityService {
    repo: PortofolioEquityRepository,
}

impl PortofolioEquityService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: PortofolioEquityRepository::new(session),
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
}
