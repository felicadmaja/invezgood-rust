use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::Local;
use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{
    extract_bearer_token, validate_session, AuthSession,
    SessionStore,
};
use worker_scrapping::on_demand;

use crate::model::EmitenTrending;
use crate::pb::emiten_trending_server::EmitenTrending as EmitenTrendingRpc;
use crate::pb::{
    GetAllEmitenTrendingFromScyllaRequest, GetAllEmitenTrendingResponse,
    GetLatestEmitenTrendingFromInvezgoRequest, GetLatestEmitenTrendingFromStockbitRequest,
};
use crate::repository::EmitenTrendingRepository;

const MOVERS_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

static LAST_MOVERS_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn movers_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_MOVERS_SCRAPE.get_or_init(|| Mutex::new(None))
}

async fn acquire_trending_refresh_slot(user_name: &str, rpc_name: &str) -> Result<(), Status> {
    let mut last = movers_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < MOVERS_SCRAPE_COOLDOWN {
            let remaining_secs = (MOVERS_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            let message = format!(
                "Rate limit: maksimal 1× / 5 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            );
            eprintln!(
                "{rpc_name} {user_name} rate-limit ditolak: sisa {remaining_secs}s"
            );
            return Err(Status::failed_precondition(message));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

#[derive(Clone)]
pub struct EmitenTrendingService {
    repo: Arc<EmitenTrendingRepository>,
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl EmitenTrendingService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            repo: Arc::new(EmitenTrendingRepository::new(Arc::clone(&session))),
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

    async fn require_admin<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let auth = self.require_auth(request).await?;
        if auth.role.trim().eq_ignore_ascii_case("admin") {
            Ok(auth)
        } else {
            Err(Status::permission_denied("Harus admin !"))
        }
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }
}

#[tonic::async_trait]
impl EmitenTrendingRpc for EmitenTrendingService {
    async fn get_all_emiten_trending_from_scylla(
        &self,
        request: Request<GetAllEmitenTrendingFromScyllaRequest>,
    ) -> Result<Response<GetAllEmitenTrendingResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllEmitenTrendingResponse>, Status> = async {
            let date_str = request.into_inner().tahun_bulan_tanggal.trim().to_string();
            if date_str.is_empty() {
                return Err(Status::invalid_argument(
                    "tahun_bulan_tanggal wajib diisi (format YYYY-MM-DD)",
                ));
            }

            let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").map_err(|_| {
                Status::invalid_argument("tahun_bulan_tanggal harus format YYYY-MM-DD")
            })?;

            let rows = self
                .repo
                .get_all_by_date(date)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

            Ok(Response::new(GetAllEmitenTrendingResponse {
                rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllEmitenTrendingFromScylla", &user_name, started);
        result
    }

    async fn get_latest_emiten_trending_from_stockbit(
        &self,
        request: Request<GetLatestEmitenTrendingFromStockbitRequest>,
    ) -> Result<Response<GetAllEmitenTrendingResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;
        let _ = request.into_inner();

        let result: Result<Response<GetAllEmitenTrendingResponse>, Status> = async {
            acquire_trending_refresh_slot(&user_name, "GetLatestEmitenTrendingFromStockbit").await?;

            on_demand::scrape_emiten_trending_movers(Arc::clone(&self.session))
                .await
                .map_err(|e| Status::internal(format!("Scrape movers gagal: {e}")))?;

            let today = Local::now().date_naive();
            let rows = self
                .repo
                .get_all_by_date(today)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

            Ok(Response::new(GetAllEmitenTrendingResponse {
                rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetLatestEmitenTrendingFromStockbit", &user_name, started);
        result
    }

    async fn get_latest_emiten_trending_from_invezgo(
        &self,
        request: Request<GetLatestEmitenTrendingFromInvezgoRequest>,
    ) -> Result<Response<GetAllEmitenTrendingResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let _ = request.into_inner();

        let result: Result<Response<GetAllEmitenTrendingResponse>, Status> = async {
            let today = Local::now().date_naive();

            let in_scrape_hours = on_demand::is_stockbit_poller_scrape_hours();
            let is_holiday = worker_scrapping::yahoo_market_holiday::is_spike_poller_holiday().await;

            if in_scrape_hours && !is_holiday {
                acquire_trending_refresh_slot(&user_name, "GetLatestEmitenTrendingFromInvezgo").await?;

                // Invezgo hanya menulis ke Scylla; response client dari MV, bukan payload API langsung.
                let _saved = crate::invezgo::fetch_and_save(Arc::clone(&self.session))
                    .await
                    .map_err(Status::internal)?;
            } else if is_holiday {
                eprintln!(
                    "GetLatestEmitenTrendingFromInvezgo {user_name}: hari libur (Sabtu/Minggu atau invezgood.hari_libur) — skip Invezgo, baca Scylla saja"
                );
            } else {
                eprintln!(
                    "GetLatestEmitenTrendingFromInvezgo {user_name}: diluar jam operasional — skip Invezgo, baca Scylla saja"
                );
            }

            let rows = self
                .repo
                .get_all_by_date(today)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

            Ok(Response::new(GetAllEmitenTrendingResponse {
                rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetLatestEmitenTrendingFromInvezgo", &user_name, started);
        result
    }
}
