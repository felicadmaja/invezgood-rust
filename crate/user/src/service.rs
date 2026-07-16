use std::sync::Arc;

use bcrypt::verify;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::jwt;
use crate::repository::UserRepository;
use crate::user_server::User as UserRpc;
use crate::{LoginRequest, LoginResponse};

pub struct UserService {
    repo: UserRepository,
}

impl UserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: UserRepository::new(session),
        }
    }
}

#[tonic::async_trait]
impl UserRpc for UserService {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();
        let email = req.email.trim().to_lowercase();
        let password = req.password;

        if email.is_empty() || password.is_empty() {
            return Err(Status::invalid_argument("email dan password wajib diisi"));
        }

        let user = self
            .repo
            .find_by_email(&email)
            .await
            .map_err(|e| Status::internal(format!("Scylla error: {e}")))?
            .ok_or_else(|| Status::unauthenticated("email atau password salah"))?;

        let hash = user.password.clone();
        let valid = tokio::task::spawn_blocking(move || verify(password, &hash))
            .await
            .map_err(|e| Status::internal(format!("bcrypt join error: {e}")))?
            .map_err(|e| Status::internal(format!("bcrypt error: {e}")))?;

        if !valid {
            return Err(Status::unauthenticated("email atau password salah"));
        }

        let (access_token, expires_in) = jwt::encode_token(&user.id, &user.email, &user.name)
            .map_err(|e| Status::internal(format!("JWT encode gagal: {e}")))?;

        Ok(Response::new(LoginResponse {
            access_token,
            expires_in,
            user_id: user.id.to_string(),
            name: user.name,
            email: user.email,
        }))
    }
}
