use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::Local;
use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::portofolio_history_server::PortofolioHistory as PortofolioHistoryRpc;
use crate::repository::PortofolioHistoryRepository;
use crate::{
    GetPortofolioHistoryByEmitenNameFromStockbitRequest,
    GetPortofolioHistoryByEmitenNameFromStockbitResponse, PortofolioHistoryRow,
};

/// Cooldown global antar invoke `GetPortofolioHistoryByEmitenNameFromStockbit` (semua user).
/// Tidak berlaku untuk `worker_scrapping` (tidak lewat RPC ini).
const HISTORY_SCRAPE_COOLDOWN: Duration = Duration::from_secs(5);

static LAST_HISTORY_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn history_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_HISTORY_SCRAPE.get_or_init(|| Mutex::new(None))
}

/// Izinkan scrape history; tolak jika < 5 detik sejak invoke terakhir (global).
async fn acquire_history_scrape_slot() -> Result<(), Status> {
    let mut last = history_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < HISTORY_SCRAPE_COOLDOWN {
            let remaining_secs = (HISTORY_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 5 detik untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet (contoh: ASBI)".into());
    }
    Ok(kode)
}

pub struct PortofolioHistoryService {
    repo: PortofolioHistoryRepository,
    session: Arc<Session>,
}

impl PortofolioHistoryService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PortofolioHistoryRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl PortofolioHistoryRpc for PortofolioHistoryService {
    async fn get_portofolio_history_by_emiten_name_from_scylla(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromStockbitRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message,
                        row: None,
                    },
                ));
            }
        };

        match self.repo.find_latest_by_emiten(&kode).await {
            Ok(Some(r)) => {
                let n = r.history.len();
                let date = r.tahun_bulan_tanggal;
                let row = Some(r.into_proto());
                let message =
                    format!("portofolio_history {kode}: {n} entri dari Scylla ({date})");
                println!(
                    "GetPortofolioHistoryByEmitenNameFromScylla {} {kode} success=true history={} {}ms",
                    username,
                    n,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: true,
                        message,
                        row,
                    },
                ))
            }
            Ok(None) => {
                let message = format!("portofolio_history {kode}: tidak ada di Scylla");
                println!(
                    "GetPortofolioHistoryByEmitenNameFromScylla {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message,
                        row: None,
                    },
                ))
            }
            Err(e) => {
                eprintln!(
                    "GetPortofolioHistoryByEmitenNameFromScylla {username}: gagal {kode}: {e}"
                );
                println!(
                    "GetPortofolioHistoryByEmitenNameFromScylla {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message: format!("baca portofolio_history gagal: {e}"),
                        row: None,
                    },
                ))
            }
        }
    }

    async fn get_portofolio_history_by_emiten_name_from_stockbit(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromStockbitRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        acquire_history_scrape_slot().await?;
        let req = request.into_inner();

        let kode = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message,
                        row: None,
                    },
                ));
            }
        };

        println!(
            "GetPortofolioHistoryByEmitenNameFromStockbit {username}: {kode} — order/v2/list + upsert portofolio_history..."
        );

        match on_demand::scrape_portofolio_history_for_emiten(Arc::clone(&self.session), &kode)
            .await
        {
            Ok(n) => {
                let today = Local::now().date_naive();
                let row = match self.repo.find_by_emiten_and_date(&kode, today).await {
                    Ok(Some(r)) => Some(r.into_proto()),
                    Ok(None) => Some(PortofolioHistoryRow {
                        emiten_name: kode.clone(),
                        tahun_bulan_tanggal: today.format("%Y-%m-%d").to_string(),
                        history: HashMap::new(),
                    }),
                    Err(e) => {
                        eprintln!(
                            "GetPortofolioHistoryByEmitenNameFromStockbit {username}: baca ulang gagal: {e}"
                        );
                        None
                    }
                };
                let message = format!(
                    "portofolio_history {kode}: scrape selesai, {n} entri di-upsert ({today})"
                );
                println!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {} {kode} success=true history={} {}ms",
                    username,
                    n,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: true,
                        message,
                        row,
                    },
                ))
            }
            Err(e) => {
                eprintln!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {username}: gagal {kode}: {e}"
                );
                println!(
                    "GetPortofolioHistoryByEmitenNameFromStockbit {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(
                    GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: false,
                        message: format!("scrape portofolio history gagal: {e}"),
                        row: None,
                    },
                ))
            }
        }
    }
}
