use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session::Session;
use grpc_stream::{send_or_break, sleep_unless_disconnected};
use stockbit_browser::{ReadinessPoller, ReadinessUpdate};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use worker_scrapping::invezgo_spike_poller::{InvezgoSpikePoller, InvezgoSpikeSnapshot};
use worker_scrapping::yahoo_spike_poller::{YahooSpikePoller, YahooSpikeSnapshot};

use crate::auth::{extract_bearer_token, validate_session, SessionStore};
use crate::model::UserRow as DbUserRow;
use crate::pb::user_server::User;
use crate::pb::{
    CekUsageRequest, GetPriceSpikeFromInvezGoRequest, GetPriceSpikeFromYahooFinanceRequest,
    GetUsersFromScyllaRequest, GetUsersFromScyllaResponse, IsStockbitReadyRequest, IsStockbitReadyResponse, LoginRequest,
    LoginResponse, LogoutRequest, LogoutResponse, PriceSpikeResponse, PriceSpikeRow,
    UsageResponse, UserRow,
};

const READY_STREAM_CHECK_SECS: u64 = 2;
const READY_STREAM_HEARTBEAT_TICKS: u64 = 15;

fn anonymous_is_stockbit_ready_response() -> IsStockbitReadyResponse {
    IsStockbitReadyResponse {
        success: false,
        message: "Login diperlukan".to_string(),
        portofolio: Vec::new(),
        is_need_login: true,
    }
}

fn need_login_status() -> Status {
    Status::unauthenticated("login diperlukan")
}

fn anonymous_price_spike_response() -> PriceSpikeResponse {
    PriceSpikeResponse {
        success: false,
        message: "Login diperlukan".to_string(),
        data: Vec::new(),
        is_need_login: true,
    }
}

async fn push_anonymous_need_login<T>(tx: &mpsc::Sender<Result<T, Status>>, ok_message: T) {
    let _ = tx.send(Ok(ok_message)).await;
    let _ = tx.send(Err(need_login_status())).await;
}

pub struct UserService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
    readiness: Arc<ReadinessPoller>,
    yahoo_spike: Arc<YahooSpikePoller>,
    invezgo_spike: Arc<InvezgoSpikePoller>,
}

impl UserService {
    pub fn new(
        session: Arc<Session>,
        auth_sessions: SessionStore,
        readiness: Arc<ReadinessPoller>,
        yahoo_spike: Arc<YahooSpikePoller>,
        invezgo_spike: Arc<InvezgoSpikePoller>,
    ) -> Self {
        Self {
            session,
            auth_sessions,
            readiness,
            yahoo_spike,
            invezgo_spike,
        }
    }

    pub fn readiness_poller(&self) -> Arc<ReadinessPoller> {
        Arc::clone(&self.readiness)
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<String, Status> {
        let token = extract_bearer_token(request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))?;
        Ok(auth.nama)
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

        let (token, auth) = crate::auth::login(&self.auth_sessions, user.clone(), &password)
            .await
            .map_err(Status::unauthenticated)?;

        Ok(Response::new(LoginResponse {
            token,
            nama: auth.nama,
            role: auth.role,
            email: user.email,
            expires_at: auth.expires_at.timestamp(),
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

    type IsStockbitReadyStream =
        Pin<Box<dyn Stream<Item = Result<IsStockbitReadyResponse, Status>> + Send>>;

    async fn is_stockbit_ready(
        &self,
        request: Request<IsStockbitReadyRequest>,
    ) -> Result<Response<Self::IsStockbitReadyStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "IsStockbitReady";

        let user_name = match extract_bearer_token(&request) {
            Ok(token) => match validate_session(&self.auth_sessions, &token).await {
                Ok(auth) => auth.nama,
                Err(_) => {
                    eprintln!(
                        "{rpc_name} anonymous {}ms — session invalid (is_need_login=true, UNAUTHENTICATED)",
                        started.elapsed().as_millis()
                    );
                    let (tx, rx) = mpsc::channel::<Result<IsStockbitReadyResponse, Status>>(2);
                    push_anonymous_need_login(&tx, anonymous_is_stockbit_ready_response()).await;
                    return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
                }
            },
            Err(_) => {
                eprintln!(
                    "{rpc_name} anonymous {}ms — tanpa auth (is_need_login=true, UNAUTHENTICATED)",
                    started.elapsed().as_millis()
                );
                let (tx, rx) = mpsc::channel::<Result<IsStockbitReadyResponse, Status>>(2);
                push_anonymous_need_login(&tx, anonymous_is_stockbit_ready_response()).await;
                return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
            }
        };
        let _ = request.into_inner();

        let readiness = Arc::clone(&self.readiness);
        let (tx, rx) = mpsc::channel::<Result<IsStockbitReadyResponse, Status>>(8);

        eprintln!("\x1b[32mIsStockbitReady {user_name}: client connect — stream dibuka\x1b[0m");

        tokio::spawn(async move {
            readiness.register_subscriber().await;

            let mut last: Option<(bool, String)> = None;
            let mut ticks_since_send: u64 = 0;

            loop {
                let update = readiness.latest().await.unwrap_or_else(|| ReadinessUpdate {
                    ready: false,
                    message: "Menunggu pengecekan berkala ke stockbit.com (interval 9–10 menit)"
                        .to_string(),
                    poll_seq: 0,
                    portofolio: Vec::new(),
                });

                let key = (update.ready, update.message.clone());
                let changed = last.as_ref() != Some(&key);
                let first = last.is_none();
                let heartbeat = ticks_since_send >= READY_STREAM_HEARTBEAT_TICKS;

                if first || changed || heartbeat {
                    if first || changed {
                        eprintln!(
                            "IsStockbitReady {user_name}: push success={} msg={:?}",
                            update.ready, update.message
                        );
                    }

                    let ok = send_or_break(
                        &tx,
                        Ok(IsStockbitReadyResponse {
                            success: update.ready,
                            message: update.message,
                            portofolio: Vec::new(),
                            is_need_login: false,
                        }),
                    )
                    .await;
                    if !ok {
                        eprintln!(
                            "\x1b[31mIsStockbitReady {user_name}: client disconnect — stream ditutup\x1b[0m"
                        );
                        break;
                    }
                    last = Some(key);
                    ticks_since_send = 0;
                } else {
                    ticks_since_send += 1;
                }

                if !sleep_unless_disconnected(
                    &tx,
                    Duration::from_secs(READY_STREAM_CHECK_SECS),
                )
                .await
                {
                    eprintln!(
                        "\x1b[31mIsStockbitReady {user_name}: client disconnect — stream ditutup\x1b[0m"
                    );
                    break;
                }
            }

            readiness.unregister_subscriber().await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type GetPriceSpikeFromYahooFinanceStream =
        Pin<Box<dyn Stream<Item = Result<PriceSpikeResponse, Status>> + Send>>;

    async fn get_price_spike_from_yahoo_finance(
        &self,
        request: Request<GetPriceSpikeFromYahooFinanceRequest>,
    ) -> Result<Response<Self::GetPriceSpikeFromYahooFinanceStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetPriceSpikeFromYahooFinance";

        let user_name = match extract_bearer_token(&request) {
            Ok(token) => match validate_session(&self.auth_sessions, &token).await {
                Ok(auth) => auth.nama,
                Err(_) => {
                    eprintln!(
                        "{rpc_name} anonymous {}ms — session invalid (is_need_login=true, UNAUTHENTICATED)",
                        started.elapsed().as_millis()
                    );
                    let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(2);
                    push_anonymous_need_login(&tx, anonymous_price_spike_response()).await;
                    return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
                }
            },
            Err(_) => {
                eprintln!(
                    "{rpc_name} anonymous {}ms — tanpa auth (is_need_login=true, UNAUTHENTICATED)",
                    started.elapsed().as_millis()
                );
                let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(2);
                push_anonymous_need_login(&tx, anonymous_price_spike_response()).await;
                return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
            }
        };
        let _ = request.into_inner();

        let poller = Arc::clone(&self.yahoo_spike);
        let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(8);

        eprintln!("\x1b[32m{rpc_name} {user_name}: client connect — stream dibuka\x1b[0m");
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );

        let user_name = user_name.clone();
        tokio::spawn(async move {
            poller.register_subscriber();
            let mut last: Option<YahooSpikeSnapshot> = None;
            let mut ticks_since_send: u64 = 0;

            loop {
                let snap = poller.latest();
                let changed = last.as_ref() != Some(&snap);
                let first = last.is_none();
                let heartbeat = ticks_since_send >= READY_STREAM_HEARTBEAT_TICKS;

                if first || changed || heartbeat {
                    let cached = worker_scrapping::yahoo_spike_cache::today_details().await;
                    if first || changed {
                        eprintln!(
                            "{rpc_name} {user_name}: push success={} msg={:?} data={:?}",
                            snap.success, snap.message, cached
                        );
                    }
                    let data = cached
                        .iter()
                        .map(|s| PriceSpikeRow {
                            emiten_name: s.emiten_name.clone(),
                            jenis_spike: s.jenis_spike.clone(),
                            value_spike_percentage: s.value_spike_percentage,
                            spike_at: s.spike_at.clone(),
                        })
                        .collect();
                    let ok = send_or_break(
                        &tx,
                        Ok(PriceSpikeResponse {
                            success: snap.success,
                            message: snap.message.clone(),
                            data,
                            is_need_login: false,
                        }),
                    )
                    .await;
                    if !ok {
                        eprintln!(
                            "\x1b[31m{rpc_name} {user_name}: client disconnect — stream ditutup\x1b[0m"
                        );
                        break;
                    }
                    last = Some(snap);
                    ticks_since_send = 0;
                } else {
                    ticks_since_send += 1;
                }

                if !sleep_unless_disconnected(
                    &tx,
                    Duration::from_secs(READY_STREAM_CHECK_SECS),
                )
                .await
                {
                    eprintln!(
                        "\x1b[31m{rpc_name} {user_name}: client disconnect — stream ditutup\x1b[0m"
                    );
                    break;
                }
            }
            poller.unregister_subscriber();
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type GetPriceSpikeFromInvezGoStream =
        Pin<Box<dyn Stream<Item = Result<PriceSpikeResponse, Status>> + Send>>;

    async fn get_price_spike_from_invez_go(
        &self,
        request: Request<GetPriceSpikeFromInvezGoRequest>,
    ) -> Result<Response<Self::GetPriceSpikeFromInvezGoStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetPriceSpikeFromInvezGo";

        let user_name = match extract_bearer_token(&request) {
            Ok(token) => match validate_session(&self.auth_sessions, &token).await {
                Ok(auth) => auth.nama,
                Err(_) => {
                    eprintln!(
                        "{rpc_name} anonymous {}ms — session invalid (is_need_login=true, UNAUTHENTICATED)",
                        started.elapsed().as_millis()
                    );
                    let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(2);
                    push_anonymous_need_login(&tx, anonymous_price_spike_response()).await;
                    return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
                }
            },
            Err(_) => {
                eprintln!(
                    "{rpc_name} anonymous {}ms — tanpa auth (is_need_login=true, UNAUTHENTICATED)",
                    started.elapsed().as_millis()
                );
                let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(2);
                push_anonymous_need_login(&tx, anonymous_price_spike_response()).await;
                return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
            }
        };
        let _ = request.into_inner();

        let poller = Arc::clone(&self.invezgo_spike);
        let (tx, rx) = mpsc::channel::<Result<PriceSpikeResponse, Status>>(8);

        eprintln!("\x1b[32m{rpc_name} {user_name}: client connect — stream dibuka\x1b[0m");
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );

        let user_name = user_name.clone();
        tokio::spawn(async move {
            poller.register_subscriber();
            let mut last: Option<InvezgoSpikeSnapshot> = None;
            let mut ticks_since_send: u64 = 0;

            loop {
                let snap = poller.latest();
                let changed = last.as_ref() != Some(&snap);
                let first = last.is_none();
                let heartbeat = ticks_since_send >= READY_STREAM_HEARTBEAT_TICKS;

                if first || changed || heartbeat {
                    let cached = worker_scrapping::invezgo_spike_cache::today_details().await;
                    if first || changed {
                        eprintln!(
                            "{rpc_name} {user_name}: push success={} msg={:?} data={:?}",
                            snap.success, snap.message, cached
                        );
                    }
                    let data = cached
                        .iter()
                        .map(|s| PriceSpikeRow {
                            emiten_name: s.emiten_name.clone(),
                            jenis_spike: s.jenis_spike.clone(),
                            value_spike_percentage: s.value_spike_percentage,
                            spike_at: s.spike_at.clone(),
                        })
                        .collect();
                    let ok = send_or_break(
                        &tx,
                        Ok(PriceSpikeResponse {
                            success: snap.success,
                            message: snap.message.clone(),
                            data,
                            is_need_login: false,
                        }),
                    )
                    .await;
                    if !ok {
                        eprintln!(
                            "\x1b[31m{rpc_name} {user_name}: client disconnect — stream ditutup\x1b[0m"
                        );
                        break;
                    }
                    last = Some(snap);
                    ticks_since_send = 0;
                } else {
                    ticks_since_send += 1;
                }

                if !sleep_unless_disconnected(
                    &tx,
                    Duration::from_secs(READY_STREAM_CHECK_SECS),
                )
                .await
                {
                    eprintln!(
                        "\x1b[31m{rpc_name} {user_name}: client disconnect — stream ditutup\x1b[0m"
                    );
                    break;
                }
            }
            poller.unregister_subscriber();
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn cek_usage(
        &self,
        request: Request<CekUsageRequest>,
    ) -> Result<Response<UsageResponse>, Status> {
        let started = std::time::Instant::now();
        let user_name = self.require_auth(&request).await?;
        let _ = request.into_inner();

        let result: Result<Response<UsageResponse>, Status> = async {
            let usage = crate::invezgo::fetch_usage()
                .await
                .map_err(Status::internal)?;
            Ok(Response::new(usage))
        }
        .await;

        eprintln!(
            "CekUsage {user_name} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
