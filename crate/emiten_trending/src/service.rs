use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Local, NaiveDate};
use rand::Rng;
use scylla::client::session::Session;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::emiten_trending_server::EmitenTrending as EmitenTrendingRpc;
use crate::model::EmitenTrending;
use crate::repository::EmitenTrendingRepository;
use crate::{
    EmitenTrendingRow, GetAllEmitenTrendingRequest, GetAllEmitenTrendingResponse,
    GetLatestEmitenTrendingFromStockbitRequest,
};

/// Interval poll Stockbit untuk stream `GetLatestEmitenTrendingFromStockbit` (detik).
const STOCKBIT_POLL_MIN_SECS: u64 = 10 * 60; // 10 menit
const STOCKBIT_POLL_MAX_SECS: u64 = 15 * 60; // 15 menit

pub struct EmitenTrendingService {
    repo: Arc<EmitenTrendingRepository>,
}

impl EmitenTrendingService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: Arc::new(EmitenTrendingRepository::new(session)),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

fn next_stockbit_poll_secs() -> u64 {
    rand::thread_rng().gen_range(STOCKBIT_POLL_MIN_SECS..=STOCKBIT_POLL_MAX_SECS)
}

#[tonic::async_trait]
impl EmitenTrendingRpc for EmitenTrendingService {
    async fn get_all_emiten_trending(
        &self,
        request: Request<GetAllEmitenTrendingRequest>,
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

        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|_| {
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
            "GetAllEmitenTrending {} {}ms",
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
        let repo = Arc::clone(&self.repo);

        println!("GetLatestEmitenTrendingFromStockbit {username}");

        let (tx, rx) = mpsc::channel::<Result<GetAllEmitenTrendingResponse, Status>>(4);

        tokio::spawn(async move {
            let mut cycle: u64 = 0;
            loop {
                cycle += 1;
                let started = Instant::now();

                // Scrape movers (Top Gainer/Loser + Freq → emiten_trending.freq) dijalankan
                // oleh `worker_scrapping` (`emiten_trending_worker::scrape_and_insert_movers`).
                // Stream ini mem-push snapshot hari ini dari Scylla (termasuk field `freq`).
                let today = Local::now().date_naive();
                let payload = match repo.get_all_by_date(today).await {
                    Ok(rows) => {
                        let with_freq = rows.iter().filter(|r| !r.freq.trim().is_empty()).count();
                        println!(
                            "GetLatestEmitenTrendingFromStockbit {username} snapshot rows={} freq_terisi={}",
                            rows.len(),
                            with_freq
                        );
                        GetAllEmitenTrendingResponse {
                            rows: rows.into_iter().map(EmitenTrending::into_proto).collect(),
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("Scylla query failed: {e}"))))
                            .await;
                        break;
                    }
                };

                let n = payload.rows.len();
                println!(
                    "GetLatestEmitenTrendingFromStockbit {username} cycle={cycle} rows={n} {}ms",
                    started.elapsed().as_millis()
                );

                if tx.send(Ok(payload)).await.is_err() {
                    // Client disconnect — hentikan poll.
                    println!("GetLatestEmitenTrendingFromStockbit {username} stream closed");
                    break;
                }

                let wait_secs = next_stockbit_poll_secs();
                println!(
                    "GetLatestEmitenTrendingFromStockbit {username} poll berikutnya dalam {wait_secs}s"
                );
                sleep(Duration::from_secs(wait_secs)).await;
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}
