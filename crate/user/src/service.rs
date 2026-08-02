use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::auth::SessionStore;
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
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
            auth_sessions,
        }
    }

    fn db_row_to_proto(row: DbUserRow) -> UserRow {
        UserRow {
            email: row.email,
            nama: row.nama.unwrap_or_default(),
            role: row.role.unwrap_or_default(),
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
            return Err(Status::invalid_argument("email dan password wajib diisi"));
        }

        let user = crate::repository::find_by_email(self.session.as_ref(), &email)
            .await
            .map_err(Status::internal)?;

        let Some(user) = user else {
            return Err(Status::unauthenticated("email atau password salah"));
        };

        let (token, auth) = crate::auth::login(&self.auth_sessions, user, &password)
            .await
            .map_err(Status::unauthenticated)?;

        Ok(Response::new(LoginResponse {
            token,
            nama: auth.nama,
            role: auth.role,
        }))
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
