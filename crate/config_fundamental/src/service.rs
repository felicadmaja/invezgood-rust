use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::ConfigFundamentalRow as DbConfigFundamentalRow;
use crate::pb::config_fundamental_server::ConfigFundamental;
use crate::pb::{
    ConfigFundamentalRow, DeleteConfigFundamentalRequest, DeleteConfigFundamentalResponse,
    GetAllConfigFundamentalRequest, GetAllConfigFundamentalResponse,
    InsertConfigFundamentalRequest, InsertConfigFundamentalResponse,
    UpdateConfigFundamentalRequest, UpdateConfigFundamentalResponse,
};

pub struct ConfigFundamentalService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl ConfigFundamentalService {
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

    fn normalize_key(key: &str) -> String {
        key.trim().to_string()
    }

    fn row_to_proto(row: DbConfigFundamentalRow) -> ConfigFundamentalRow {
        ConfigFundamentalRow {
            key: Self::normalize_key(&row.key),
            value: row.value,
            description: row.description.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl ConfigFundamental for ConfigFundamentalService {
    async fn get_all_config_fundamental(
        &self,
        request: Request<GetAllConfigFundamentalRequest>,
    ) -> Result<Response<GetAllConfigFundamentalResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetAllConfigFundamental";

        let user_name = match self.require_auth(&request).await {
            Ok(auth) => auth.nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let result: Result<Response<GetAllConfigFundamentalResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = crate::repository::find_all(self.session.as_ref())
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetAllConfigFundamentalResponse {
                rows: rows.into_iter().map(Self::row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug(rpc_name, &user_name, started);
        result
    }

    async fn update_config_fundamental(
        &self,
        request: Request<UpdateConfigFundamentalRequest>,
    ) -> Result<Response<UpdateConfigFundamentalResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "UpdateConfigFundamental";

        let user_name = match self.require_auth(&request).await {
            Ok(auth) => auth.nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let result: Result<Response<UpdateConfigFundamentalResponse>, Status> = async {
            let req = request.into_inner();
            let key = Self::normalize_key(&req.key);
            if key.is_empty() {
                return Ok(Response::new(UpdateConfigFundamentalResponse {
                    success: false,
                    message: "key wajib diisi".to_string(),
                }));
            }

            let description = req.description.trim().to_string();
            let updated = crate::repository::update(
                self.session.as_ref(),
                &key,
                req.value,
                &description,
            )
            .await
            .map_err(Status::internal)?;

            let (success, message) = if updated {
                (
                    true,
                    format!("config_fundamental key={key} berhasil diupdate"),
                )
            } else {
                (
                    false,
                    format!("config_fundamental key={key} tidak ditemukan"),
                )
            };

            Ok(Response::new(UpdateConfigFundamentalResponse {
                success,
                message,
            }))
        }
        .await;

        Self::log_rpc_debug(rpc_name, &user_name, started);
        result
    }

    async fn insert_config_fundamental(
        &self,
        request: Request<InsertConfigFundamentalRequest>,
    ) -> Result<Response<InsertConfigFundamentalResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "InsertConfigFundamental";

        let user_name = match self.require_auth(&request).await {
            Ok(auth) => auth.nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let result: Result<Response<InsertConfigFundamentalResponse>, Status> = async {
            let req = request.into_inner();
            let key = Self::normalize_key(&req.key);
            if key.is_empty() {
                return Ok(Response::new(InsertConfigFundamentalResponse {
                    success: false,
                    message: "key wajib diisi".to_string(),
                }));
            }

            let description = req.description.trim().to_string();
            let inserted = crate::repository::insert(
                self.session.as_ref(),
                &key,
                req.value,
                &description,
            )
            .await
            .map_err(Status::internal)?;

            let (success, message) = if inserted {
                (
                    true,
                    format!("config_fundamental key={key} berhasil diinsert"),
                )
            } else {
                (
                    false,
                    format!("config_fundamental key={key} sudah ada"),
                )
            };

            Ok(Response::new(InsertConfigFundamentalResponse {
                success,
                message,
            }))
        }
        .await;

        Self::log_rpc_debug(rpc_name, &user_name, started);
        result
    }

    async fn delete_config_fundamental(
        &self,
        request: Request<DeleteConfigFundamentalRequest>,
    ) -> Result<Response<DeleteConfigFundamentalResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "DeleteConfigFundamental";

        let user_name = match self.require_auth(&request).await {
            Ok(auth) => auth.nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let result: Result<Response<DeleteConfigFundamentalResponse>, Status> = async {
            let key = Self::normalize_key(&request.into_inner().key);
            if key.is_empty() {
                return Ok(Response::new(DeleteConfigFundamentalResponse {
                    success: false,
                    message: "key wajib diisi".to_string(),
                }));
            }

            let deleted = crate::repository::delete(self.session.as_ref(), &key)
                .await
                .map_err(Status::internal)?;

            let (success, message) = if deleted {
                (
                    true,
                    format!("config_fundamental key={key} berhasil dihapus"),
                )
            } else {
                (
                    false,
                    format!("config_fundamental key={key} tidak ditemukan"),
                )
            };

            Ok(Response::new(DeleteConfigFundamentalResponse {
                success,
                message,
            }))
        }
        .await;

        Self::log_rpc_debug(rpc_name, &user_name, started);
        result
    }
}
