use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::PortofolioEquity;
use crate::pb::portofolio_equity_server::PortofolioEquity as PortofolioEquityRpc;
use crate::pb::{
    GetAllPortofolioEquityFromScyllaRequest, GetAllPortofolioEquityFromScyllaResponse,
    PortofolioEquityRow,
};
use crate::repository::PortofolioEquityRepository;

pub struct PortofolioEquityService {
    repo: PortofolioEquityRepository,
    auth_sessions: SessionStore,
}

impl PortofolioEquityService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            repo: PortofolioEquityRepository::new(session),
            auth_sessions,
        }
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

    fn rows_to_proto(rows: Vec<PortofolioEquity>) -> Vec<PortofolioEquityRow> {
        rows.into_iter().map(PortofolioEquity::into_proto).collect()
    }
}

#[tonic::async_trait]
impl PortofolioEquityRpc for PortofolioEquityService {
    async fn get_all_portofolio_equity_from_scylla(
        &self,
        request: Request<GetAllPortofolioEquityFromScyllaRequest>,
    ) -> Result<Response<GetAllPortofolioEquityFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllPortofolioEquityFromScyllaResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = self.repo.get_all().await.map_err(Status::internal)?;
            Ok(Response::new(GetAllPortofolioEquityFromScyllaResponse {
                rows: Self::rows_to_proto(rows),
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllPortofolioEquityFromScylla", &user_name, started);
        result
    }
}
