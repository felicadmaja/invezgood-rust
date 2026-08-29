use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::cache::MedianCache;
use crate::pb::ev_to_ebit_server::EvToEbit;
use crate::pb::{GetMedianEvToEbitdaRequest, GetMedianEvToEbitdaResponse};

pub struct EvToEbitService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
    cache: Arc<MedianCache>,
}

impl EvToEbitService {
    pub fn new(
        session: Arc<Session>,
        auth_sessions: SessionStore,
        cache: Arc<MedianCache>,
    ) -> Self {
        Self {
            session,
            auth_sessions,
            cache,
        }
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }
}

#[tonic::async_trait]
impl EvToEbit for EvToEbitService {
    async fn get_median_ev_to_ebitda(
        &self,
        request: Request<GetMedianEvToEbitdaRequest>,
    ) -> Result<Response<GetMedianEvToEbitdaResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetMedianEVToEbitda";

        let user_name = match self.require_auth(&request).await {
            Ok(auth) => auth.nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let result: Result<Response<GetMedianEvToEbitdaResponse>, Status> = async {
            let _inner = request.into_inner();
            let cached = self
                .cache
                .get_or_compute(Arc::clone(&self.session))
                .await
                .map_err(Status::internal)?;
            Ok(Response::new((*cached).clone()))
        }
        .await;

        Self::log_rpc_debug(rpc_name, &user_name, started);
        result
    }
}
