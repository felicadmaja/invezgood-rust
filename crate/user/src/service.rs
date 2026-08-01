use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::auth::{new_session_store, SessionStore};
use crate::model::UserRow as DbUserRow;
use crate::pb::user_server::User;
use crate::pb::{
    GetUsersFromScyllaRequest, GetUsersFromScyllaResponse, LoginRequest, LoginResponse,
    LogoutRequest, LogoutResponse, UserRow,
};

pub struct UserService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl UserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            auth_sessions: new_session_store(),
        }
    }

    fn db_row_to_proto(row: DbUserRow) -> UserRow {
        UserRow {
            email: row.email,
            nama: row.nama.unwrap_or_default(),
            role: row.role.unwrap_or_default(),
        }
    }

    fn auth_to_proto(auth: &crate::auth::AuthSession) -> UserRow {
        UserRow {
            email: auth.email.clone(),
            nama: auth.nama.clone(),
            role: auth.role.clone(),
        }
    }
}

#[tonic::async_trait]
impl User for UserService {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let LoginRequest { email, password } = request.into_inner();

        if email.is_empty() || password.is_empty() {
            return Ok(Response::new(LoginResponse {
                success: false,
                message: "email dan password wajib diisi".into(),
                token: String::new(),
                user: None,
            }));
        }

        let user = crate::repository::find_by_email(self.session.as_ref(), &email)
            .await
            .map_err(Status::internal)?;

        let Some(user) = user else {
            return Ok(Response::new(LoginResponse {
                success: false,
                message: "email atau password salah".into(),
                token: String::new(),
                user: None,
            }));
        };

        match crate::auth::login(&self.auth_sessions, user, &password).await {
            Ok((token, auth)) => Ok(Response::new(LoginResponse {
                success: true,
                message: "login berhasil".into(),
                token,
                user: Some(Self::auth_to_proto(&auth)),
            })),
            Err(message) => Ok(Response::new(LoginResponse {
                success: false,
                message,
                token: String::new(),
                user: None,
            })),
        }
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        let token = request.into_inner().token;

        if token.is_empty() {
            return Ok(Response::new(LogoutResponse {
                success: false,
                message: "token wajib diisi".into(),
            }));
        }

        let removed = crate::auth::logout(&self.auth_sessions, &token).await;

        Ok(Response::new(LogoutResponse {
            success: true,
            message: if removed {
                "logout berhasil".into()
            } else {
                "session tidak ditemukan (sudah logout)".into()
            },
        }))
    }

    async fn get_users_from_scylla(
        &self,
        _request: Request<GetUsersFromScyllaRequest>,
    ) -> Result<Response<GetUsersFromScyllaResponse>, Status> {
        let rows = crate::repository::token_ring_scan(self.session.as_ref())
            .await
            .map_err(Status::internal)?;

        let items = rows.into_iter().map(Self::db_row_to_proto).collect();

        Ok(Response::new(GetUsersFromScyllaResponse { items }))
    }
}
