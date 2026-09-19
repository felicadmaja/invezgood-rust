use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::compute::compute_median;
use crate::pb::ev_to_ebit_server::EvToEbit;
use crate::yahoo::YahooClient;
use crate::pb::{
    GetMedianEvToEbitdaFromScyllaRequest, GetMedianEvToEbitdaFromScyllaResponse,
    GetMedianEvToEbitdaFromYahooFinanceRequest, GetMedianEvToEbitdaFromYahooFinanceResponse,
};
use crate::repository;
use crate::sync::persist_median_response;

pub struct EvToEbitService {
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
}

impl EvToEbitService {
    pub fn new(session: Arc<Session>, yahoo: Arc<YahooClient>) -> Self {
        Self { session, yahoo }
    }

    /// Logic RPC `GetMedianEVToEbitdaFromYahooFinance` — fetch Yahoo setiap invoke, upsert Scylla, tanpa auth gRPC.
    pub async fn fetch_median_from_yahoo_finance(
        &self,
    ) -> Result<GetMedianEvToEbitdaFromYahooFinanceResponse, String> {
        let resp = compute_median(Arc::clone(&self.session), Arc::clone(&self.yahoo)).await?;
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
    async fn get_median_ev_to_ebitda_from_yahoo_finance(
        &self,
        request: Request<GetMedianEvToEbitdaFromYahooFinanceRequest>,
    ) -> Result<Response<GetMedianEvToEbitdaFromYahooFinanceResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetMedianEVToEbitdaFromYahooFinance";

        let user_name = "anonymous";
        let result: Result<Response<GetMedianEvToEbitdaFromYahooFinanceResponse>, Status> = async {
            let _inner = request.into_inner();
            self.fetch_median_from_yahoo_finance()
                .await
                .map(Response::new)
                .map_err(Status::internal)
        }
        .await;

        Self::log_rpc_debug(rpc_name, user_name, started);
        result
    }

    async fn get_median_ev_to_ebitda_from_scylla(
        &self,
        request: Request<GetMedianEvToEbitdaFromScyllaRequest>,
    ) -> Result<Response<GetMedianEvToEbitdaFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetMedianEVToEbitdaFromScylla";
        let user_name = "anonymous";

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
