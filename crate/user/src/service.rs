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

use crate::auth::{require_admin, require_auth};
use crate::jwt;
use crate::repository::UserRepository;
use crate::user_server::User as UserRpc;
use crate::{
    AddUserRequest, AddUserResponse, DeleteUserRequest, DeleteUserResponse, GetAllUsersRequest,
    GetAllUsersResponse, IsStockbitReadyRequest, IsStockbitReadyResponse, LoginRequest,
    LoginResponse, RoleUser, UpdatePasswordRequest, UpdatePasswordResponse, UpdateRoleUserRequest,
    UpdateRoleUserResponse, UserRow as UserProtoRow,
};

/// Interval cek cache poller untuk stream subscriber (detik).
const READY_STREAM_CHECK_SECS: u64 = 2;
/// Kirim ulang status yang sama setiap N tick (~30s) agar koneksi tetap “hidup”.
const READY_STREAM_HEARTBEAT_TICKS: u64 = 15;

/// Parse UUID proto bytes: 16-byte binary, atau UTF-8 string UUID.
fn parse_uuid_bytes(raw: &[u8], field: &str) -> Result<Uuid, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument(format!("{field} wajib diisi")));
    }
    if raw.len() == 16 {
        return Uuid::from_slice(raw).map_err(|e| {
            Status::invalid_argument(format!("{field} UUID binary tidak valid: {e}"))
        });
    }
    let s = std::str::from_utf8(raw)
        .map_err(|_| {
            Status::invalid_argument(format!("{field} harus UUID 16-byte atau string UTF-8"))
        })?
        .trim();
    Uuid::parse_str(s)
        .map_err(|e| Status::invalid_argument(format!("{field} UUID string tidak valid: {e}")))
}

/// Scylla `user.role` text → enum. Kosong / unknown → VIEWER.
fn role_from_storage(raw: &str) -> RoleUser {
    match raw.trim().to_ascii_lowercase().as_str() {
        "admin" => RoleUser::Admin,
        "viewer" | "user" | "" => RoleUser::Viewer,
        _ => RoleUser::Viewer,
    }
}

/// Enum → text untuk Scylla + JWT claim.
fn role_to_storage(role: RoleUser) -> &'static str {
    match role {
        RoleUser::Admin => "admin",
        RoleUser::Viewer | RoleUser::Unspecified => "viewer",
    }
}

/// Request AddUser: UNSPECIFIED → VIEWER.
fn role_from_request(raw: i32) -> Result<RoleUser, Status> {
    match RoleUser::try_from(raw).unwrap_or(RoleUser::Unspecified) {
        RoleUser::Unspecified => Ok(RoleUser::Viewer),
        RoleUser::Admin => Ok(RoleUser::Admin),
        RoleUser::Viewer => Ok(RoleUser::Viewer),
    }
}

/// Request UpdateRoleUser: wajib ADMIN atau VIEWER.
fn role_from_update_request(raw: i32) -> Result<RoleUser, Status> {
    match RoleUser::try_from(raw).unwrap_or(RoleUser::Unspecified) {
        RoleUser::Admin => Ok(RoleUser::Admin),
        RoleUser::Viewer => Ok(RoleUser::Viewer),
        RoleUser::Unspecified => Err(Status::invalid_argument(
            "role wajib RoleUser ADMIN atau VIEWER",
        )),
    }
}

pub struct UserService {
    repo: UserRepository,
    readiness: Arc<ReadinessPoller>,
}

impl UserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: UserRepository::new(session),
            // Background poller: cek stockbit.com acak setiap 9–15 menit.
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
        let role = role_from_storage(&user.role);
        let role_storage = role_to_storage(role);
        let (access_token, expires_in) =
            jwt::encode_token(&user.id, &email, &user.name, role_storage)
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
            role: role.into(),
        }))
    }

    async fn update_password(
        &self,
        request: Request<UpdatePasswordRequest>,
    ) -> Result<Response<UpdatePasswordResponse>, Status> {
        let claims = require_auth(&request)?;
        let req = request.into_inner();
        let user_id = parse_uuid_bytes(&req.user_id, "user_id")?;
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

    async fn add_user(
        &self,
        request: Request<AddUserRequest>,
    ) -> Result<Response<AddUserResponse>, Status> {
        let claims = require_admin(&request)?;
        let req = request.into_inner();
        let email = req.email.trim().to_lowercase();
        let name = req.name.trim().to_string();
        let password = req.password;
        let role = role_from_request(req.role)?;
        let role_storage = role_to_storage(role);

        if email.is_empty() || name.is_empty() || password.trim().is_empty() {
            return Ok(Response::new(AddUserResponse {
                success: false,
                message: "email, password, dan name wajib diisi".to_string(),
            }));
        }
        if password.len() < 8 {
            return Ok(Response::new(AddUserResponse {
                success: false,
                message: "password minimal 8 karakter".to_string(),
            }));
        }

        let existing = self
            .repo
            .find_by_email(&email)
            .await
            .map_err(|e| Status::internal(format!("Scylla error: {e}")))?;
        if existing.is_some() {
            return Ok(Response::new(AddUserResponse {
                success: false,
                message: format!("email sudah terpakai: {email}"),
            }));
        }

        let password_hash = tokio::task::spawn_blocking(move || hash(password, DEFAULT_COST))
            .await
            .map_err(|e| Status::internal(format!("bcrypt join error: {e}")))?
            .map_err(|e| Status::internal(format!("bcrypt hash error: {e}")))?;

        let id = Uuid::new_v4();
        self.repo
            .insert_user(id, &name, &email, &password_hash, role_storage)
            .await
            .map_err(|e| Status::internal(format!("Scylla insert user gagal: {e}")))?;

        println!(
            "AddUser by {}: {email} id={id} role={role_storage}",
            claims.name
        );

        Ok(Response::new(AddUserResponse {
            success: true,
            message: format!("user ditambahkan: {email} (id={id}, role={role_storage})"),
        }))
    }

    async fn delete_user(
        &self,
        request: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
        let claims = require_admin(&request)?;
        let req = request.into_inner();
        let id = parse_uuid_bytes(&req.id, "id")?;

        if claims.user_id == id.to_string() {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                message: "tidak boleh menghapus akun sendiri".to_string(),
            }));
        }

        let exists = self
            .repo
            .find_by_id(id)
            .await
            .map_err(|e| Status::internal(format!("Scylla error: {e}")))?
            .is_some();
        if !exists {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                message: "user tidak ditemukan".to_string(),
            }));
        }

        self.repo
            .delete_user(id)
            .await
            .map_err(|e| Status::internal(format!("Scylla delete user gagal: {e}")))?;

        println!("DeleteUser by {}: id={id}", claims.name);

        Ok(Response::new(DeleteUserResponse {
            success: true,
            message: format!("user dihapus: {id}"),
        }))
    }

    async fn update_role_user(
        &self,
        request: Request<UpdateRoleUserRequest>,
    ) -> Result<Response<UpdateRoleUserResponse>, Status> {
        let claims = require_admin(&request)?;
        let req = request.into_inner();
        let user_id = parse_uuid_bytes(&req.user_id, "user_id")?;
        let role = role_from_update_request(req.role)?;
        let role_storage = role_to_storage(role);

        let exists = self
            .repo
            .find_by_id(user_id)
            .await
            .map_err(|e| Status::internal(format!("Scylla error: {e}")))?
            .is_some();
        if !exists {
            return Ok(Response::new(UpdateRoleUserResponse {
                success: false,
                message: "Role user gagal diubah".to_string(),
            }));
        }

        match self.repo.update_role(user_id, role_storage).await {
            Ok(()) => {
                println!(
                    "UpdateRoleUser by {}: id={user_id} role={role_storage}",
                    claims.name
                );
                Ok(Response::new(UpdateRoleUserResponse {
                    success: true,
                    message: "Role user berhasil diubah".to_string(),
                }))
            }
            Err(e) => {
                eprintln!("UpdateRoleUser {user_id}: gagal: {e}");
                Ok(Response::new(UpdateRoleUserResponse {
                    success: false,
                    message: "Role user gagal diubah".to_string(),
                }))
            }
        }
    }

    async fn get_all_users(
        &self,
        request: Request<GetAllUsersRequest>,
    ) -> Result<Response<GetAllUsersResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let is_admin = claims.role.trim().eq_ignore_ascii_case("admin");

        let proto_rows: Vec<UserProtoRow> = if is_admin {
            let rows = self
                .repo
                .get_all()
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
            rows.into_iter()
                .map(|r| UserProtoRow {
                    id: r.id.to_string(),
                    name: r.name,
                    email: r.email.trim().to_lowercase(),
                    role: role_from_storage(&r.role).into(),
                })
                .collect()
        } else {
            let user_id = Uuid::parse_str(claims.user_id.trim()).map_err(|e| {
                Status::unauthenticated(format!("JWT user_id tidak valid: {e}"))
            })?;
            match self
                .repo
                .find_by_id(user_id)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            {
                Some(r) => vec![UserProtoRow {
                    id: r.id.to_string(),
                    name: r.name,
                    email: r.email.trim().to_lowercase(),
                    role: role_from_storage(&r.role).into(),
                }],
                None => vec![],
            }
        };

        println!(
            "GetAllUsers {} admin={} rows={} {}ms",
            claims.name,
            is_admin,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllUsersResponse { rows: proto_rows }))
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
                        message: "Menunggu pengecekan berkala ke stockbit.com (interval 9–15 menit)"
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
