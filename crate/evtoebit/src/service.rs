use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use grpc_stream::send_or_break;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::compute::compute_median;
use crate::pb::ev_to_ebit_server::EvToEbit;
use crate::yahoo::YahooClient;
use crate::pb::{
    GetMedianEvToEbitdaFromScyllaRequest, GetMedianEvToEbitdaFromScyllaResponse,
    GetMedianEvToEbitdaFromYahooFinanceRequest, GetMedianEvToEbitdaFromYahooFinanceResponse,
};
use crate::repository;
use crate::sync::persist_median_response;

type YahooFinanceStream =
    Pin<Box<dyn Stream<Item = Result<GetMedianEvToEbitdaFromYahooFinanceResponse, Status>> + Send>>;

pub struct EvToEbitService {
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
    auth_sessions: SessionStore,
}

impl EvToEbitService {
    pub fn new(
        session: Arc<Session>,
        yahoo: Arc<YahooClient>,
        auth_sessions: SessionStore,
    ) -> Self {
        Self {
            session,
            yahoo,
            auth_sessions,
        }
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    /// Logic RPC `GetMedianEVToEbitdaFromYahooFinance` — fetch Yahoo, upsert Scylla (tanpa stream; dev/seed).
    pub async fn fetch_median_from_yahoo_finance(
        &self,
    ) -> Result<GetMedianEvToEbitdaFromYahooFinanceResponse, String> {
        let resp = compute_median(Arc::clone(&self.session), Arc::clone(&self.yahoo), None).await?;
        let n = persist_median_response(self.session.as_ref(), &resp).await?;
        let mut out = resp;
        out.message = format!("{}; upsert {n} baris ke invezgood.evtoebit", out.message);
        Ok(out)
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
    type GetMedianEVToEbitdaFromYahooFinanceStream = YahooFinanceStream;

    async fn get_median_ev_to_ebitda_from_yahoo_finance(
        &self,
        request: Request<GetMedianEvToEbitdaFromYahooFinanceRequest>,
    ) -> Result<Response<YahooFinanceStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetMedianEVToEbitdaFromYahooFinance";

        let auth = match self.require_auth(&request).await {
            Ok(auth) => auth,
            Err(status) => {
                Self::log_rpc_debug(rpc_name, "anonymous", started);
                return Err(status);
            }
        };
        let user_name = auth.nama.clone();
        let _inner = request.into_inner();

        let session = Arc::clone(&self.session);
        let yahoo = Arc::clone(&self.yahoo);
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            let run = async {
                let resp = compute_median(session.clone(), yahoo, Some(stream_tx.clone())).await?;
                let n = persist_median_response(session.as_ref(), &resp).await?;
                let mut final_resp = resp;
                final_resp.message =
                    format!("{}; upsert {n} baris ke invezgood.evtoebit", final_resp.message);
                if !send_or_break(&stream_tx, Ok(final_resp)).await {
                    return Err("client disconnect".to_string());
                }
                Ok(())
            }
            .await;

            if let Err(e) = run {
                if e != "client disconnect" {
                    let _ = send_or_break(&stream_tx, Err(Status::internal(e))).await;
                }
            }

            Self::log_rpc_debug(rpc_name, &user_name, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(stream_rx)) as YahooFinanceStream
        ))
    }

    async fn get_median_ev_to_ebitda_from_scylla(
        &self,
        request: Request<GetMedianEvToEbitdaFromScyllaRequest>,
    ) -> Result<Response<GetMedianEvToEbitdaFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetMedianEVToEbitdaFromScylla";

        let auth = match self.require_auth(&request).await {
            Ok(auth) => auth,
            Err(status) => {
                Self::log_rpc_debug(rpc_name, "anonymous", started);
                return Err(status);
            }
        };
        let user_name = auth.nama.as_str();

        let result: Result<Response<GetMedianEvToEbitdaFromScyllaResponse>, Status> = async {
            let _inner = request.into_inner();
            let db_rows = repository::find_all(self.session.as_ref())
                .await
                .map_err(Status::internal)?;
            let rows: Vec<_> = db_rows.iter().map(repository::row_to_pb).collect();
            Ok(Response::new(GetMedianEvToEbitdaFromScyllaResponse {
                success: true,
                message: format!("{} baris dari invezgood.evtoebit", rows.len()),
                rows,
            }))
        }
        .await;

        Self::log_rpc_debug(rpc_name, user_name, started);
        result
    }
}
