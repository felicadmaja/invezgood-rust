use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{Datelike, Local, Timelike};
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
    GetLatestEmitenTrendingFromStockbitRequest,
};
use crate::repository::EmitenTrendingRepository;

const MOVERS_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

static LAST_MOVERS_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn movers_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_MOVERS_SCRAPE.get_or_init(|| Mutex::new(None))
}

async fn acquire_movers_scrape_slot(user_name: &str) -> Result<(), Status> {
    let mut last = movers_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < MOVERS_SCRAPE_COOLDOWN {
            let remaining_secs = (MOVERS_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            let message = format!(
                "Rate limit: maksimal 1× / 5 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            );
            eprintln!(
                "GetLatestEmitenTrendingFromStockbit {user_name} rate-limit ditolak: sisa {remaining_secs}s"
            );
            return Err(Status::failed_precondition(message));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

/// Batasi scrape emiten trending ke Senin–Jumat, jam 08:15–12:00 dan 13:30–16:15 (server lokal).
fn require_emiten_trending_scrape_hours() -> Result<(), Status> {
    let now = Local::now();
    match now.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => {
            return Err(Status::failed_precondition(
                "Diluar hari operasional Senin-Jumat (Sabtu/Minggu tidak scrape)",
            ));
        }
        _ => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 8 * 60 + 15;
    const MORNING_END: u32 = 12 * 60 + 1;
    const AFTERNOON_START: u32 = 13 * 60 + 30;
    const AFTERNOON_END: u32 = 16 * 60 + 16;
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    if !in_morning && !in_afternoon {
        return Err(Status::failed_precondition(
            "Diluar jam 08:15-12:00 dan 13:30-16:15",
        ));
    }
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
            require_emiten_trending_scrape_hours()?;
            acquire_movers_scrape_slot(&user_name).await?;

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
}
