use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::emiten_trending_count_server::EmitenTrendingCount as EmitenTrendingCountRpc;
use crate::model::EmitenTrendingCountByName;
use crate::repository::EmitenTrendingCountRepository;
use crate::{
    EmitenTrendingCountByNameRow, GetMostTrendingEmitenRequest, GetMostTrendingEmitenResponse,
};

pub struct EmitenTrendingCountService {
    repo: EmitenTrendingCountRepository,
}

impl EmitenTrendingCountService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: EmitenTrendingCountRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl EmitenTrendingCountRpc for EmitenTrendingCountService {
    async fn get_most_trending_emiten(
        &self,
        request: Request<GetMostTrendingEmitenRequest>,
    ) -> Result<Response<GetMostTrendingEmitenResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows: Vec<EmitenTrendingCountByName> = self
            .repo
            .get_most_trending()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenTrendingCountByNameRow> = rows
            .into_iter()
            .map(EmitenTrendingCountByName::into_proto)
            .collect();

        println!(
            "GetMostTrendingEmiten {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetMostTrendingEmitenResponse {
            rows: proto_rows,
        }))
    }
}
