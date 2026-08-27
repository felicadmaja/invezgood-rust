use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::ShareholderCompositionRow as DbShareholderCompositionRow;
use crate::pb::shareholder_composition_server::ShareholderComposition;
use crate::pb::{
    GetShareholderCompositionByCodeRequest, GetShareholderCompositionByCodeResponse,
    ShareholderCompositionRow,
};

pub struct ShareholderCompositionService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl ShareholderCompositionService {
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

    fn db_row_to_proto(row: DbShareholderCompositionRow) -> ShareholderCompositionRow {
        ShareholderCompositionRow {
            code: row.code,
            tahun_bulan: row.tahun_bulan,
            detail: crate::repository::row_to_proto_detail(row.detail),
        }
    }
}

#[tonic::async_trait]
impl ShareholderComposition for ShareholderCompositionService {
    async fn get_shareholder_composition_by_code(
        &self,
        request: Request<GetShareholderCompositionByCodeRequest>,
    ) -> Result<Response<GetShareholderCompositionByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetShareholderCompositionByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let saved = crate::invezgo::fetch_and_save(self.session.clone(), &code)
                .await
                .map_err(Status::internal)?;

            let rows = crate::repository::find_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetShareholderCompositionByCodeResponse {
                success: true,
                message: format!("{saved} baris di-upsert dari Invezgo, {} baris dari Scylla", rows.len()),
                rows: rows.into_iter().map(Self::db_row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetShareholderCompositionByCode", &user_name, started);
        result
    }
}
