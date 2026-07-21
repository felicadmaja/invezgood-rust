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
use user::{require_auth, require_stockbit_scrape_hours};
use worker_scrapping::on_demand;

use crate::emiten_trending_server::EmitenTrending as EmitenTrendingRpc;
use crate::model::EmitenTrending;
use crate::repository::EmitenTrendingRepository;
use crate::{
    EmitenTrendingRow, GetAllEmitenTrendingFromScyllaRequest, GetAllEmitenTrendingResponse,
    GetLatestEmitenTrendingFromStockbitRequest,
};

/// Cooldown global antar invoke `GetLatestEmitenTrendingFromStockbit` (semua user).
const MOVERS_SCRAPE_COOLDOWN: Duration = Duration::from_secs(60);

static LAST_MOVERS_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn movers_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_MOVERS_SCRAPE.get_or_init(|| Mutex::new(None))
}

/// Izinkan scrape movers; tolak jika < 1 menit sejak invoke terakhir (global, semua user).
async fn acquire_movers_scrape_slot() -> Result<(), Status> {
    let mut last = movers_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < MOVERS_SCRAPE_COOLDOWN {
            let remaining_secs = (MOVERS_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 1 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
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
}

#[tonic::async_trait]
impl EmitenTrendingRpc for EmitenTrendingService {
    async fn get_all_emiten_trending_from_scylla(
        &self,
        request: Request<GetAllEmitenTrendingFromScyllaRequest>,
    ) -> Result<Response<GetAllEmitenTrendingResponse>, Status> {
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

        let rows: Vec<EmitenTrending> = self
            .repo
            .get_all_by_date(date)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenTrendingRow> =
            rows.into_iter().map(EmitenTrending::into_proto).collect();

        println!(
            "GetAllEmitenTrendingFromScylla {} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllEmitenTrendingResponse {
            rows: proto_rows,
        }))
    }

    type GetLatestEmitenTrendingFromStockbitStream =
        Pin<Box<dyn Stream<Item = Result<GetAllEmitenTrendingResponse, Status>> + Send>>;

    async fn get_latest_emiten_trending_from_stockbit(
        &self,
        request: Request<GetLatestEmitenTrendingFromStockbitRequest>,
    ) -> Result<Response<Self::GetLatestEmitenTrendingFromStockbitStream>, Status> {
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        require_stockbit_scrape_hours()?;
        acquire_movers_scrape_slot().await?;

        println!("GetLatestEmitenTrendingFromStockbit {username}: on-demand scrape movers...");
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
