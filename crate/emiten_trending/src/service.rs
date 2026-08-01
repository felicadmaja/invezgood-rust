use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::Local;
use scylla::client::session::Session;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use user::{require_admin, require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::emiten_trending_server::EmitenTrending as EmitenTrendingRpc;
use crate::model::EmitenTrending;
use crate::repository::EmitenTrendingRepository;
use crate::{
    GetAllEmitenTrendingFromScyllaRequest, GetAllEmitenTrendingResponse,
    GetLatestEmitenTrendingFromStockbitRequest,
};

/// Cooldown global antar invoke `GetLatestEmitenTrendingFromStockbit` (semua user).
const MOVERS_SCRAPE_COOLDOWN: Duration = Duration::from_secs(3 * 60);

static LAST_MOVERS_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn movers_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_MOVERS_SCRAPE.get_or_init(|| Mutex::new(None))
}

/// Izinkan scrape movers; tolak jika < 5 menit sejak invoke terakhir (global, semua user).
async fn acquire_movers_scrape_slot() -> Result<(), Status> {
    let mut last = movers_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < MOVERS_SCRAPE_COOLDOWN {
            let remaining_secs = (MOVERS_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 3 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

pub struct EmitenTrendingService {
    repo: Arc<EmitenTrendingRepository>,
    session: Arc<Session>,
}

impl EmitenTrendingService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: Arc::new(EmitenTrendingRepository::new(Arc::clone(&session))),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }

    /// Rate limit 1×/3 menit + scrape movers (sama RPC). Jam 07–17 dicek pemanggil.
    /// Dipakai juga auto `IsStockbitReady` — jatah rate limit terpakai bersama user RPC.
    pub async fn scrape_from_stockbit_if_allowed(&self) -> Result<(), Status> {
        acquire_movers_scrape_slot().await?;
        on_demand::scrape_emiten_trending_movers(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(e))?;
        Ok(())
    }

    /// Auto poller: movers saja (tanpa full keystats), lock Chrome background.
    pub async fn scrape_from_stockbit_if_allowed_background(&self) -> Result<(), Status> {
        acquire_movers_scrape_slot().await?;
        on_demand::scrape_emiten_trending_movers_background(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(e))?;
        Ok(())
    }
}

/// Interval push snapshot Scylla ke client yang subscribe.
const SCYLLA_SUBSCRIBE_INTERVAL: Duration = Duration::from_secs(6 * 60);

#[tonic::async_trait]
impl EmitenTrendingRpc for EmitenTrendingService {
    type GetAllEmitenTrendingFromScyllaStream =
        Pin<Box<dyn Stream<Item = Result<GetAllEmitenTrendingResponse, Status>> + Send>>;

    async fn get_all_emiten_trending_from_scylla(
        &self,
        request: Request<GetAllEmitenTrendingFromScyllaRequest>,
    ) -> Result<Response<Self::GetAllEmitenTrendingFromScyllaStream>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();
        let date_str = req.tahun_bulan_tanggal.trim();

        if date_str.is_empty() {
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal wajib diisi (format YYYY-MM-DD)",
            ));
        }

        let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument("tahun_bulan_tanggal harus format YYYY-MM-DD")
        })?;

        let date_label = date.format("%Y-%m-%d").to_string();
        let repo = Arc::clone(&self.repo);
        let (tx, rx) = mpsc::channel::<Result<GetAllEmitenTrendingResponse, Status>>(2);

        println!(
            "GetAllEmitenTrendingFromScylla: client subscribe user={username} tanggal={date_label} {}ms",
            started.elapsed().as_millis()
        );

        tokio::spawn(async move {
            loop {
                let tick_started = Instant::now();
                match repo.get_all_by_date(date).await {
                    Ok(rows) => {
                        let n = rows.len();
                        let payload = GetAllEmitenTrendingResponse {
                            rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
                        };
                        println!(
                            "GetAllEmitenTrendingFromScylla: push user={username} tanggal={date_label} rows={n} {}ms",
                            tick_started.elapsed().as_millis()
                        );
                        if tx.send(Ok(payload)).await.is_err() {
                            println!(
                                "GetAllEmitenTrendingFromScylla: client unsubscribe/disconnect user={username} tanggal={date_label}"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("Scylla query failed: {e}"))))
                            .await;
                        break;
                    }
                }

                tokio::select! {
                    _ = tx.closed() => {
                        println!(
                            "GetAllEmitenTrendingFromScylla: client unsubscribe/disconnect user={username} tanggal={date_label}"
                        );
                        break;
                    }
                    _ = tokio::time::sleep(SCYLLA_SUBSCRIBE_INTERVAL) => {}
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type GetLatestEmitenTrendingFromStockbitStream =
        Pin<Box<dyn Stream<Item = Result<GetAllEmitenTrendingResponse, Status>> + Send>>;

    async fn get_latest_emiten_trending_from_stockbit(
        &self,
        request: Request<GetLatestEmitenTrendingFromStockbitRequest>,
    ) -> Result<Response<Self::GetLatestEmitenTrendingFromStockbitStream>, Status> {
        let claims = require_admin(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        require_stockbit_scrape_hours()?;
        acquire_movers_scrape_slot().await?;

        println!("GetLatestEmitenTrendingFromStockbit {username}: on-demand scrape movers + key_stats...");
        let started = Instant::now();

        on_demand::scrape_emiten_trending_movers(Arc::clone(&self.session))
            .await
            .map_err(|e| Status::internal(format!("Scrape movers gagal: {e}")))?;

        let today = Local::now().date_naive();
        let rows = self
            .repo
            .get_all_by_date(today)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let with_freq = rows.iter().filter(|r| !r.freq.trim().is_empty()).count();
        let payload = GetAllEmitenTrendingResponse {
            rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
        };
        let n = payload.rows.len();
        println!(
            "GetLatestEmitenTrendingFromStockbit {username} rows={n} freq_terisi={with_freq} {}ms",
            started.elapsed().as_millis()
        );

        let (tx, rx) = mpsc::channel(1);
        if tx.send(Ok(payload)).await.is_err() {
            return Err(Status::internal("stream closed before send"));
        }

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
