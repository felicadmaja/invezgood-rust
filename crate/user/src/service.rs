use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bcrypt::{hash, verify, DEFAULT_COST};
use scylla::client::session::Session;
use stockbit_browser::ReadinessPoller;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::auth::require_auth;
use crate::jwt;
use crate::repository::UserRepository;
use crate::user_server::User as UserRpc;
use crate::{
    IsStockbitReadyRequest, IsStockbitReadyResponse, LoginRequest, LoginResponse,
    UpdatePasswordRequest, UpdatePasswordResponse,
};

/// Interval cek cache poller untuk stream subscriber (detik).
const READY_STREAM_CHECK_SECS: u64 = 2;
/// Kirim ulang status yang sama setiap N tick (~30s) agar koneksi tetap “hidup”.
const READY_STREAM_HEARTBEAT_TICKS: u64 = 15;

/// Parse `user_id` proto bytes: UUID 16-byte binary, atau UTF-8 string UUID.
fn parse_user_id_bytes(raw: &[u8]) -> Result<Uuid, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument("user_id wajib diisi"));
    }
    if raw.len() == 16 {
        return Uuid::from_slice(raw)
            .map_err(|e| Status::invalid_argument(format!("user_id UUID binary tidak valid: {e}")));
    }
    let s = std::str::from_utf8(raw)
        .map_err(|_| Status::invalid_argument("user_id harus UUID 16-byte atau string UTF-8"))?
        .trim();
    Uuid::parse_str(s)
        .map_err(|e| Status::invalid_argument(format!("user_id UUID string tidak valid: {e}")))
}

pub struct UserService {
    repo: UserRepository,
    readiness: Arc<ReadinessPoller>,
}

impl UserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: UserRepository::new(session),
            // Background poller: cek stockbit.com acak setiap 50–60 menit.
            // RPC stream hanya membaca Redis (`stockbit:readiness`) — tidak hit web langsung.
            readiness: ReadinessPoller::start(),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared_statements().await
    }

    /// Poller readiness (untuk subscribe auto-scrape / monitoring).
    pub fn readiness_poller(&self) -> Arc<ReadinessPoller> {
        Arc::clone(&self.readiness)
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

    async fn update_password(
        &self,
        request: Request<UpdatePasswordRequest>,
    ) -> Result<Response<UpdatePasswordResponse>, Status> {
        let claims = require_auth(&request)?;
        let req = request.into_inner();
        let user_id = parse_user_id_bytes(&req.user_id)?;
        let new_password = req.new_password;

        if new_password.trim().is_empty() {
            return Ok(Response::new(UpdatePasswordResponse {
                success: false,
                message: "new_password wajib diisi".to_string(),
            }));
        }
        if new_password.len() < 8 {
            return Ok(Response::new(UpdatePasswordResponse {
                success: false,
                message: "new_password minimal 8 karakter".to_string(),
            }));
        }

        if claims.user_id != user_id.to_string() {
            return Ok(Response::new(UpdatePasswordResponse {
                success: false,
                message: "tidak boleh mengubah password user lain".to_string(),
            }));
        }

        let exists = self
            .repo
            .find_by_id(user_id)
            .await
            .map_err(|e| Status::internal(format!("Scylla error: {e}")))?
            .is_some();
        if !exists {
            return Ok(Response::new(UpdatePasswordResponse {
                success: false,
                message: "user tidak ditemukan".to_string(),
            }));
        }

        let password_hash = tokio::task::spawn_blocking(move || hash(new_password, DEFAULT_COST))
            .await
            .map_err(|e| Status::internal(format!("bcrypt join error: {e}")))?
            .map_err(|e| Status::internal(format!("bcrypt hash error: {e}")))?;

        self.repo
            .update_password(user_id, &password_hash)
            .await
            .map_err(|e| Status::internal(format!("Scylla update password gagal: {e}")))?;

        println!("UpdatePassword {} ok", claims.name);

        Ok(Response::new(UpdatePasswordResponse {
            success: true,
            message: "password berhasil diubah".to_string(),
        }))
    }

    type IsStockbitReadyStream =
        Pin<Box<dyn Stream<Item = Result<IsStockbitReadyResponse, Status>> + Send>>;

    async fn is_stockbit_ready(
        &self,
        request: Request<IsStockbitReadyRequest>,
    ) -> Result<Response<Self::IsStockbitReadyStream>, Status> {
        let claims = require_auth(&request)?;
        let user_name = claims.name.clone();
        if user_name.trim().is_empty() {
            return Err(Status::unauthenticated("nama user tidak ada di JWT"));
        }

        let readiness = Arc::clone(&self.readiness);
        let (tx, rx) = mpsc::channel::<Result<IsStockbitReadyResponse, Status>>(8);

        println!("IsStockbitReady {user_name}: stream dibuka (subscribe)");

        tokio::spawn(async move {
            let mut last: Option<(bool, String)> = None;
            let mut ticks_since_send: u64 = 0;

            loop {
                let update = readiness.latest().await.unwrap_or_else(|| {
                    stockbit_browser::ReadinessUpdate {
                        ready: false,
                        message: "Menunggu pengecekan berkala ke stockbit.com (interval 50–60 menit)"
                            .to_string(),
                        poll_seq: 0,
                    }
                });

                let key = (update.ready, update.message.clone());
                let changed = last.as_ref() != Some(&key);
                let first = last.is_none();
                let heartbeat = ticks_since_send >= READY_STREAM_HEARTBEAT_TICKS;

                if first || changed || heartbeat {
                    if first || changed {
                        println!(
                            "IsStockbitReady {user_name}: push ready={} msg={:?}",
                            update.ready, update.message
                        );
                    }

                    let ok = tx
                        .send(Ok(IsStockbitReadyResponse {
                            ready: update.ready,
                            message: update.message,
                        }))
                        .await
                        .is_ok();
                    if !ok {
                        println!("IsStockbitReady {user_name}: client disconnect — stream ditutup");
                        break;
                    }
                    last = Some(key);
                    ticks_since_send = 0;
                } else {
                    ticks_since_send += 1;
                }

                sleep(Duration::from_secs(READY_STREAM_CHECK_SECS)).await;
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
