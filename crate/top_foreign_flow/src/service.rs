use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::TopForeignFlowRow as DbTopForeignFlowRow;
use crate::pb::top_foreign_flow_server::TopForeignFlow;
use crate::pb::{
    GetTopForeignFlowByCodeRequest, GetTopForeignFlowByCodeResponse,
    GetTopForeignFlowByTanggalRequest, GetTopForeignFlowByTanggalResponse, TopForeignFlowRow,
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
    async fn get_top_foreign_flow_by_tanggal(
        &self,
        request: Request<GetTopForeignFlowByTanggalRequest>,
    ) -> Result<Response<GetTopForeignFlowByTanggalResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetTopForeignFlowByTanggalResponse>, Status> = async {
            let inner = request.into_inner();
            let trade_date = crate::invezgo::parse_trade_date(&inner.tahun_bulan_tanggal)
                .map_err(Status::invalid_argument)?;

            crate::invezgo::ensure_not_today(trade_date)
                .map_err(Status::failed_precondition)?;

            crate::invezgo::ensure_not_weekend(trade_date)
                .map_err(Status::failed_precondition)?;

            let cached = crate::repository::exists_by_date_mv(self.session.as_ref(), trade_date)
                .await
                .map_err(Status::internal)?;

            let saved = if cached {
                eprintln!(
                    "GetTopForeignFlowByTanggal {user_name} skip Invezgo API date={trade_date} (MV ada ≥1 baris)"
                );
                0
            } else {
                crate::invezgo::fetch_and_save(self.session.clone(), trade_date)
                    .await
                    .map_err(Status::internal)?
            };

            let rows = crate::repository::find_by_date(self.session.as_ref(), trade_date)
                .await
                .map_err(Status::internal)?;

            let message = if cached {
                format!(
                    "cache Scylla: Invezgo API dilewati, {} baris dari Scylla",
                    rows.len()
                )
            } else {
                format!(
                    "{saved} baris di-upsert dari Invezgo, {} baris dari Scylla",
                    rows.len()
                )
            };

            Ok(Response::new(GetTopForeignFlowByTanggalResponse {
                success: true,
                message,
                items: rows.into_iter().map(Self::db_row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetTopForeignFlowByTanggal", &user_name, started);
        result
    }

    async fn get_top_foreign_flow_by_code(
        &self,
        request: Request<GetTopForeignFlowByCodeRequest>,
    ) -> Result<Response<GetTopForeignFlowByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetTopForeignFlowByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let rows = crate::repository::find_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetTopForeignFlowByCodeResponse {
                success: true,
                message: format!("{} baris top foreign flow code={code}", rows.len()),
                items: rows.into_iter().map(Self::db_row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetTopForeignFlowByCode", &user_name, started);
        result
    }
}
