use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::Portofolio;
use crate::portofolio_server::Portofolio as PortofolioRpc;
use crate::repository::PortofolioRepository;
use crate::{GetAllPortofolioRequest, GetAllPortofolioResponse, PortofolioRow};

pub struct PortofolioService {
    repo: PortofolioRepository,
}

impl PortofolioService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: PortofolioRepository::new(session),
        }
    }
}

#[tonic::async_trait]
impl PortofolioRpc for PortofolioService {
    async fn get_all_portofolio(
        &self,
        request: Request<GetAllPortofolioRequest>,
    ) -> Result<Response<GetAllPortofolioResponse>, Status> {
        let claims = require_auth(&request)?;
        eprintln!(
            "GetAllPortofolio oleh user={} email={}",
            claims.name, claims.email
        );

        let rows: Vec<Portofolio> = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<PortofolioRow> = rows.into_iter().map(Portofolio::into_proto).collect();

        Ok(Response::new(GetAllPortofolioResponse {
            rows: proto_rows,
        }))
    }
}
