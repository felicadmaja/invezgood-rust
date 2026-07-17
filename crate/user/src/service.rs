use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bcrypt::verify;
use scylla::client::session::Session;
use stockbit_browser::ReadinessPoller;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::auth::require_auth;
use crate::jwt;
use crate::repository::UserRepository;
use crate::user_server::User as UserRpc;
use crate::{IsStockbitReadyRequest, IsStockbitReadyResponse, LoginRequest, LoginResponse};

pub struct UserService {
    repo: UserRepository,
    readiness: Arc<ReadinessPoller>,
}

impl UserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: UserRepository::new(session),
            // Background poller: cek stockbit.com setiap 3600–7200s.
            // RPC hanya membaca cache — tidak hit web langsung.
            readiness: ReadinessPoller::start(),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared_statements().await
    }
}

#[tonic::async_trait]
impl UserRpc for UserService {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let started = Instant::now();
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

        let email = user.email.trim().to_lowercase();
        let (access_token, expires_in) = jwt::encode_token(&user.id, &email, &user.name)
            .map_err(|e| Status::internal(format!("JWT encode gagal: {e}")))?;

        println!(
            "Login {} {}ms",
            user.name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(LoginResponse {
            access_token,
            expires_in,
            user_id: user.id.to_string(),
            name: user.name,
            email,
        }))
    }

    type IsStockbitReadyStream =
        Pin<Box<dyn Stream<Item = Result<IsStockbitReadyResponse, Status>> + Send>>;

    async fn is_stockbit_ready(
        &self,
        request: Request<IsStockbitReadyRequest>,
    ) -> Result<Response<Self::IsStockbitReadyStream>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let user_name = claims.name.trim();
        if user_name.is_empty() {
            return Err(Status::unauthenticated("nama user tidak ada di JWT"));
        }

        // Ikuti pola pooling: jangan hit stockbit.com dari RPC.
        let update = self.readiness.latest().await.unwrap_or_else(|| {
            stockbit_browser::ReadinessUpdate {
                ready: false,
                message: "Menunggu pengecekan berkala ke stockbit.com (interval 3600–7200s)"
                    .to_string(),
            }
        });

        let stream = tokio_stream::once(Ok(IsStockbitReadyResponse {
            ready: update.ready,
            message: update.message,
        }));

        println!(
            "IsStockbitReady {user_name} {}ms (from poller cache)",
            started.elapsed().as_millis()
        );

        Ok(Response::new(Box::pin(stream)))
    }
}
