use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::TopForeignFlowRow as DbTopForeignFlowRow;
use crate::pb::top_foreign_flow_server::TopForeignFlow;
use crate::pb::{
    GetTopForeignFlowRequest, GetTopForeignFlowResponse, TopForeignFlowRow,
};

pub struct TopForeignFlowService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl TopForeignFlowService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
            auth_sessions,
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

    fn db_row_to_proto(row: DbTopForeignFlowRow) -> TopForeignFlowRow {
        TopForeignFlowRow {
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            code: row.code,
            name: row.name.unwrap_or_default(),
            price: row.price.unwrap_or_default(),
            change: row.change.unwrap_or_default(),
            value: row.value,
            volume: row.volume.unwrap_or_default(),
            accum_or_dist: row.accum_or_dist.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl TopForeignFlow for TopForeignFlowService {
    async fn get_top_foreign_flow(
        &self,
        request: Request<GetTopForeignFlowRequest>,
    ) -> Result<Response<GetTopForeignFlowResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetTopForeignFlowResponse>, Status> = async {
            let trade_date =
                crate::invezgo::resolve_trade_date(request.into_inner().tahun_bulan_tanggal)
                    .map_err(Status::invalid_argument)?;

            crate::invezgo::ensure_not_weekend(trade_date)
                .map_err(Status::failed_precondition)?;

            let saved = crate::invezgo::fetch_and_save(self.session.clone(), trade_date)
                .await
                .map_err(Status::internal)?;

            let rows = crate::repository::find_by_date(self.session.as_ref(), trade_date)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetTopForeignFlowResponse {
                success: true,
                message: format!("{saved} baris di-upsert dari Invezgo, {read} baris dari Scylla", read = rows.len()),
                items: rows.into_iter().map(Self::db_row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetTopForeignFlow", &user_name, started);
        result
    }
}
