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
            if inner.tahun_bulan_tanggal.is_empty() {
                return Err(Status::invalid_argument(
                    "tahun_bulan_tanggal wajib diisi (≥1 tanggal YYYY-MM-DD)",
                ));
            }

            let mut all_rows = Vec::new();
            let mut saved_total = 0usize;
            let mut cached_dates = 0usize;
            let mut fetched_dates = 0usize;

            for raw_date in &inner.tahun_bulan_tanggal {
                let trade_date = crate::invezgo::parse_trade_date(raw_date)
                    .map_err(Status::invalid_argument)?;

                let outcome = crate::sync::sync_trade_date(self.session.clone(), trade_date)
                    .await
                    .map_err(|e| {
                        if e.contains("Sabtu") || e.contains("hari ini") {
                            Status::failed_precondition(e)
                        } else {
                            Status::internal(e)
                        }
                    })?;

                if outcome.cached {
                    eprintln!(
                        "GetTopForeignFlowByTanggal {user_name} skip Invezgo API date={trade_date} (MV ada ≥1 baris)"
                    );
                    cached_dates += 1;
                } else {
                    saved_total += outcome.saved;
                    fetched_dates += 1;
                }

                all_rows.extend(outcome.rows);
            }

            let message = format!(
                "{} tanggal ({} dari cache Scylla, {} fetch Invezgo {saved_total} baris upsert), {} baris total",
                inner.tahun_bulan_tanggal.len(),
                cached_dates,
                fetched_dates,
                all_rows.len()
            );

            Ok(Response::new(GetTopForeignFlowByTanggalResponse {
                success: true,
                message,
                items: all_rows.into_iter().map(Self::db_row_to_proto).collect(),
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
