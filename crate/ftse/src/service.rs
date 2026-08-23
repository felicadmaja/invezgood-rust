use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::FtseRow;
use crate::pb::ftse_server::Ftse;
use crate::pb::{
    DeleteFtseRequest, DeleteFtseResponse, FtseRow as ProtoFtseRow, GetAllFtseRequest,
    GetAllFtseResponse, InsertFtseRequest, InsertFtseResponse, UpdateFtseRequest,
    UpdateFtseResponse,
};

pub struct FtseService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl FtseService {
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

    fn normalize_code(raw: &str) -> Result<String, String> {
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(format!(
                "code tidak valid ({raw}); wajib tepat 4 huruf alphabet"
            ));
        }
        Ok(code)
    }

    fn row_to_proto(row: FtseRow) -> ProtoFtseRow {
        ProtoFtseRow {
            code: row.code,
            grade: row.grade.unwrap_or_default(),
            status: row.status.unwrap_or_default(),
            updated_at: row
                .updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
        }
    }

    fn validate_grade_status(grade: &str, status: &str) -> Result<(), String> {
        if grade.trim().is_empty() {
            return Err("grade wajib diisi".to_string());
        }
        if status.trim().is_empty() {
            return Err("status wajib diisi".to_string());
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Ftse for FtseService {
    async fn get_all_ftse(
        &self,
        request: Request<GetAllFtseRequest>,
    ) -> Result<Response<GetAllFtseResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetAllFtse";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<GetAllFtseResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();
            let _ = request.into_inner();

            match crate::repository::find_all(self.session.as_ref()).await {
                Ok(rows) => {
                    let n = rows.len();
                    Ok(Response::new(GetAllFtseResponse {
                        success: true,
                        message: format!("ftse: {n} baris"),
                        data: rows.into_iter().map(Self::row_to_proto).collect(),
                    }))
                }
                Err(e) => Ok(Response::new(GetAllFtseResponse {
                    success: false,
                    message: e,
                    data: vec![],
                })),
            }
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn insert_ftse(
        &self,
        request: Request<InsertFtseRequest>,
    ) -> Result<Response<InsertFtseResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "InsertFtse";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<InsertFtseResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let req = request.into_inner();
            let code = match Self::normalize_code(&req.code) {
                Ok(c) => c,
                Err(error) => {
                    return Ok(Response::new(InsertFtseResponse {
                        success: false,
                        message: error,
                    }));
                }
            };
            if let Err(error) = Self::validate_grade_status(&req.grade, &req.status) {
                return Ok(Response::new(InsertFtseResponse {
                    success: false,
                    message: error,
                }));
            }

            let row = FtseRow {
                code,
                grade: Some(req.grade.trim().to_string()),
                status: Some(req.status.trim().to_string()),
                updated_at: Some(Utc::now()),
            };

            match crate::repository::upsert(self.session.as_ref(), &row).await {
                Ok(()) => Ok(Response::new(InsertFtseResponse {
                    success: true,
                    message: String::new(),
                })),
                Err(error) => Ok(Response::new(InsertFtseResponse {
                    success: false,
                    message: error,
                })),
            }
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn update_ftse(
        &self,
        request: Request<UpdateFtseRequest>,
    ) -> Result<Response<UpdateFtseResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "UpdateFtse";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<UpdateFtseResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let req = request.into_inner();
            let code = match Self::normalize_code(&req.code) {
                Ok(c) => c,
                Err(error) => {
                    return Ok(Response::new(UpdateFtseResponse {
                        success: false,
                        message: error,
                    }));
                }
            };
            if let Err(error) = Self::validate_grade_status(&req.grade, &req.status) {
                return Ok(Response::new(UpdateFtseResponse {
                    success: false,
                    message: error,
                }));
            }

            match crate::repository::find_by_code(self.session.as_ref(), &code).await {
                Ok(None) => {
                    return Ok(Response::new(UpdateFtseResponse {
                        success: false,
                        message: format!("ftse {code} tidak ditemukan"),
                    }));
                }
                Ok(Some(_)) => {}
                Err(error) => {
                    return Ok(Response::new(UpdateFtseResponse {
                        success: false,
                        message: error,
                    }));
                }
            }

            let row = FtseRow {
                code,
                grade: Some(req.grade.trim().to_string()),
                status: Some(req.status.trim().to_string()),
                updated_at: Some(Utc::now()),
            };

            match crate::repository::update(self.session.as_ref(), &row).await {
                Ok(()) => Ok(Response::new(UpdateFtseResponse {
                    success: true,
                    message: String::new(),
                })),
                Err(error) => Ok(Response::new(UpdateFtseResponse {
                    success: false,
                    message: error,
                })),
            }
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn delete_ftse(
        &self,
        request: Request<DeleteFtseRequest>,
    ) -> Result<Response<DeleteFtseResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "DeleteFtse";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<DeleteFtseResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let code = match Self::normalize_code(&request.into_inner().code) {
                Ok(c) => c,
                Err(message) => {
                    return Ok(Response::new(DeleteFtseResponse {
                        success: false,
                        message,
                    }));
                }
            };

            match crate::repository::find_by_code(self.session.as_ref(), &code).await {
                Ok(None) => {
                    return Ok(Response::new(DeleteFtseResponse {
                        success: false,
                        message: format!("ftse {code} tidak ditemukan"),
                    }));
                }
                Ok(Some(_)) => {}
                Err(message) => {
                    return Ok(Response::new(DeleteFtseResponse {
                        success: false,
                        message,
                    }));
                }
            }

            match crate::repository::delete_by_code(self.session.as_ref(), &code).await {
                Ok(()) => Ok(Response::new(DeleteFtseResponse {
                    success: true,
                    message: format!("ftse {code} dihapus"),
                })),
                Err(message) => Ok(Response::new(DeleteFtseResponse {
                    success: false,
                    message,
                })),
            }
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
