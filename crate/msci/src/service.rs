use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::MsciRow;
use crate::pb::msci_server::Msci;
use crate::pb::{
    DeleteMsciRequest, DeleteMsciResponse, GetAllMsciRequest, GetAllMsciResponse,
    InsertMsciRequest, InsertMsciResponse, MsciRow as ProtoMsciRow, UpdateMsciRequest,
    UpdateMsciResponse,
};

pub struct MsciService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl MsciService {
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

    fn row_to_proto(row: MsciRow) -> ProtoMsciRow {
        ProtoMsciRow {
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

    fn parse_updated_at(raw: &str) -> Result<DateTime<Utc>, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Utc::now());
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
            return Ok(dt.with_timezone(&Utc));
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
            return Ok(naive.and_utc());
        }
        Err(format!(
            "updated_at tidak valid ({raw}); gunakan RFC3339 atau YYYY-MM-DD HH:MM:SS"
        ))
    }
}

#[tonic::async_trait]
impl Msci for MsciService {
    async fn get_all_msci(
        &self,
        request: Request<GetAllMsciRequest>,
    ) -> Result<Response<GetAllMsciResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetAllMsci";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<GetAllMsciResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();
            let _ = request.into_inner();

            match crate::repository::find_all(self.session.as_ref()).await {
                Ok(rows) => {
                    let n = rows.len();
                    Ok(Response::new(GetAllMsciResponse {
                        success: true,
                        message: format!("msci: {n} baris"),
                        data: rows.into_iter().map(Self::row_to_proto).collect(),
                    }))
                }
                Err(e) => Ok(Response::new(GetAllMsciResponse {
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

    async fn insert_msci(
        &self,
        request: Request<InsertMsciRequest>,
    ) -> Result<Response<InsertMsciResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "InsertMsci";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<InsertMsciResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let req = request.into_inner();
            let code = match Self::normalize_code(&req.code) {
                Ok(c) => c,
                Err(error) => {
                    return Ok(Response::new(InsertMsciResponse {
                        success: false,
                        message: error,
                    }));
                }
            };
            if let Err(error) = Self::validate_grade_status(&req.grade, &req.status) {
                return Ok(Response::new(InsertMsciResponse {
                    success: false,
                    message: error,
                }));
            }
            let updated_at = match Self::parse_updated_at(&req.updated_at) {
                Ok(ts) => ts,
                Err(error) => {
                    return Ok(Response::new(InsertMsciResponse {
                        success: false,
                        message: error,
                    }));
                }
            };

            let row = MsciRow {
                code,
                grade: Some(req.grade.trim().to_string()),
                status: Some(req.status.trim().to_string()),
                updated_at: Some(updated_at),
            };

            match crate::repository::upsert(self.session.as_ref(), &row).await {
                Ok(()) => Ok(Response::new(InsertMsciResponse {
                    success: true,
                    message: String::new(),
                })),
                Err(error) => Ok(Response::new(InsertMsciResponse {
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

    async fn update_msci(
        &self,
        request: Request<UpdateMsciRequest>,
    ) -> Result<Response<UpdateMsciResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "UpdateMsci";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<UpdateMsciResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let req = request.into_inner();
            let code = match Self::normalize_code(&req.code) {
                Ok(c) => c,
                Err(error) => {
                    return Ok(Response::new(UpdateMsciResponse {
                        success: false,
                        message: error,
                    }));
                }
            };
            if let Err(error) = Self::validate_grade_status(&req.grade, &req.status) {
                return Ok(Response::new(UpdateMsciResponse {
                    success: false,
                    message: error,
                }));
            }
            let updated_at = match Self::parse_updated_at(&req.updated_at) {
                Ok(ts) => ts,
                Err(error) => {
                    return Ok(Response::new(UpdateMsciResponse {
                        success: false,
                        message: error,
                    }));
                }
            };

            match crate::repository::find_by_code(self.session.as_ref(), &code).await {
                Ok(None) => {
                    return Ok(Response::new(UpdateMsciResponse {
                        success: false,
                        message: format!("msci {code} tidak ditemukan"),
                    }));
                }
                Ok(Some(_)) => {}
                Err(error) => {
                    return Ok(Response::new(UpdateMsciResponse {
                        success: false,
                        message: error,
                    }));
                }
            }

            let row = MsciRow {
                code,
                grade: Some(req.grade.trim().to_string()),
                status: Some(req.status.trim().to_string()),
                updated_at: Some(updated_at),
            };

            match crate::repository::update(self.session.as_ref(), &row).await {
                Ok(()) => Ok(Response::new(UpdateMsciResponse {
                    success: true,
                    message: String::new(),
                })),
                Err(error) => Ok(Response::new(UpdateMsciResponse {
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

    async fn delete_msci(
        &self,
        request: Request<DeleteMsciRequest>,
    ) -> Result<Response<DeleteMsciResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "DeleteMsci";
        let mut user_name = "anonymous".to_string();

        let result: Result<Response<DeleteMsciResponse>, Status> = async {
            let auth = self.require_auth(&request).await?;
            user_name = auth.nama.clone();

            let code = match Self::normalize_code(&request.into_inner().code) {
                Ok(c) => c,
                Err(message) => {
                    return Ok(Response::new(DeleteMsciResponse {
                        success: false,
                        message,
                    }));
                }
            };

            match crate::repository::find_by_code(self.session.as_ref(), &code).await {
                Ok(None) => {
                    return Ok(Response::new(DeleteMsciResponse {
                        success: false,
                        message: format!("msci {code} tidak ditemukan"),
                    }));
                }
                Ok(Some(_)) => {}
                Err(message) => {
                    return Ok(Response::new(DeleteMsciResponse {
                        success: false,
                        message,
                    }));
                }
            }

            match crate::repository::delete_by_code(self.session.as_ref(), &code).await {
                Ok(()) => Ok(Response::new(DeleteMsciResponse {
                    success: true,
                    message: format!("msci {code} dihapus"),
                })),
                Err(message) => Ok(Response::new(DeleteMsciResponse {
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
