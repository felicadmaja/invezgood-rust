use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};
use worker_scrapping::on_demand;

use crate::pb::portofolio_history_server::PortofolioHistory as PortofolioHistoryRpc;
use crate::pb::{
    GetPortofolioHistoryByEmitenNameFromScyllaRequest,
    GetPortofolioHistoryByEmitenNameFromScyllaResponse,
    GetPortofolioHistoryByEmitenNameFromStockbitRequest,
    GetPortofolioHistoryByEmitenNameFromStockbitResponse,
    GetPortofolioHistoryByTahunBulanFromScyllaRequest,
    GetPortofolioHistoryByTahunBulanFromScyllaResponse,
};
use crate::repository::PortofolioHistoryRepository;

const HISTORY_SCRAPE_COOLDOWN: Duration = Duration::from_secs(1);

static LAST_HISTORY_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn history_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_HISTORY_SCRAPE.get_or_init(|| Mutex::new(None))
}

async fn acquire_history_scrape_slot() -> Result<(), Status> {
    let mut last = history_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < HISTORY_SCRAPE_COOLDOWN {
            let remaining_secs = (HISTORY_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 1 detik untuk semua user. Tunggu {remaining_secs} detik lagi"
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

fn parse_tahun_bulan(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("tahun_bulan wajib diisi (YYYY-MM)".into());
    }
    chrono::NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d").map_err(|_| {
        format!("tahun_bulan tidak valid (harus YYYY-MM): {value}")
    })?;
    Ok(value.to_string())
}

pub struct PortofolioHistoryService {
    repo: PortofolioHistoryRepository,
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl PortofolioHistoryService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PortofolioHistoryRepository::new(session_for_repo),
            session,
            auth_sessions,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }
}

#[tonic::async_trait]
impl PortofolioHistoryRpc for PortofolioHistoryService {
    async fn get_portofolio_history_by_emiten_name_from_scylla(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromScyllaRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromScyllaResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetPortofolioHistoryByEmitenNameFromScyllaResponse>, Status> =
            async {
                let req = request.into_inner();
                let kode = match parse_emiten_name(&req.emiten_name) {
                    Ok(c) => c,
                    Err(message) => {
                        return Ok(Response::new(
                            GetPortofolioHistoryByEmitenNameFromScyllaResponse {
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
                        Ok(Response::new(
                            GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                                success: true,
                                message: format!(
                                    "portofolio_history {kode}: {n} entri dari Scylla ({date})"
                                ),
                                row: Some(r.into_proto()),
                            },
                        ))
                    }
                    Ok(None) => Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                            success: false,
                            message: format!("portofolio_history {kode}: tidak ada di Scylla"),
                            row: None,
                        },
                    )),
                    Err(e) => Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                            success: false,
                            message: format!("baca portofolio_history gagal: {e}"),
                            row: None,
                        },
                    )),
                }
            }
            .await;

        Self::log_rpc_debug(
            "GetPortofolioHistoryByEmitenNameFromScylla",
            &user_name,
            started,
        );
        result
    }

    async fn get_portofolio_history_by_tahun_bulan_from_scylla(
        &self,
        request: Request<GetPortofolioHistoryByTahunBulanFromScyllaRequest>,
    ) -> Result<Response<GetPortofolioHistoryByTahunBulanFromScyllaResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let mut emiten_log = String::new();
        let result: Result<Response<GetPortofolioHistoryByTahunBulanFromScyllaResponse>, Status> =
            async {
                let req = request.into_inner();
                let tahun_bulan = match parse_tahun_bulan(&req.tahun_bulan) {
                    Ok(v) => v,
                    Err(message) => {
                        return Ok(Response::new(
                            GetPortofolioHistoryByTahunBulanFromScyllaResponse {
                                success: false,
                                message,
                                rows: vec![],
                            },
                        ));
                    }
                };

                match self.repo.find_by_tahun_bulan(&tahun_bulan).await {
                    Ok(rows) => {
                        let mut seen = std::collections::HashSet::new();
                        let names: Vec<String> = rows
                            .iter()
                            .map(|r| r.emiten_name.trim().to_ascii_uppercase())
                            .filter(|n| !n.is_empty() && seen.insert(n.clone()))
                            .collect();
                        emiten_log = names.join(",");
                        let n = rows.len();
                        Ok(Response::new(
                            GetPortofolioHistoryByTahunBulanFromScyllaResponse {
                                success: true,
                                message: format!(
                                    "portofolio_history {tahun_bulan}: {n} baris dari Scylla"
                                ),
                                rows: rows.into_iter().map(|r| r.into_proto()).collect(),
                            },
                        ))
                    }
                    Err(e) => Ok(Response::new(
                        GetPortofolioHistoryByTahunBulanFromScyllaResponse {
                            success: false,
                            message: format!("baca portofolio_history gagal: {e}"),
                            rows: vec![],
                        },
                    )),
                }
            }
            .await;

        let elapsed = started.elapsed().as_millis();
        if emiten_log.is_empty() {
            eprintln!(
                "GetPortofolioHistoryByTahunBulanFromScylla {user_name} {elapsed}ms"
            );
        } else {
            eprintln!(
                "GetPortofolioHistoryByTahunBulanFromScylla {user_name} {elapsed}ms - {emiten_log}"
            );
        }
        result
    }

    async fn get_portofolio_history_by_emiten_name_from_stockbit(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromStockbitRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        enum LogSource {
            Cache,
            Api,
            Other,
        }

        let (result, log_source, log_emiten) = async {
            let req = request.into_inner();
            let kode = match parse_emiten_name(&req.emiten_name) {
                Ok(c) => c,
                Err(message) => {
                    return (
                        Ok(Response::new(
                            GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                                success: false,
                                message,
                                row: None,
                            },
                        )),
                        LogSource::Other,
                        String::new(),
                    );
                }
            };

            if let Some(mut cached) = crate::redis_cache::get(&kode).await {
                cached.message = format!(
                    "{} (redis cache)",
                    cached.message.trim_end_matches(" (redis cache)")
                );
                return (Ok(Response::new(cached)), LogSource::Cache, kode);
            }

            if let Err(status) = acquire_history_scrape_slot().await {
                return (Err(status), LogSource::Other, kode);
            }

            match on_demand::scrape_portofolio_history_for_emiten(
                Arc::clone(&self.session),
                &kode,
            )
            .await
            {
                Ok(n) => {
                    let row = match self.repo.find_latest_by_emiten(&kode).await {
                        Ok(Some(r)) => Some(r.into_proto()),
                        Ok(None) => None,
                        Err(e) => {
                            eprintln!(
                                "GetPortofolioHistoryByEmitenNameFromStockbit: baca ulang gagal: {e}"
                            );
                            None
                        }
                    };
                    let date_note = row
                        .as_ref()
                        .map(|r| r.tahun_bulan_tanggal.as_str())
                        .unwrap_or("-");
                    let resp = GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                        success: true,
                        message: format!(
                            "portofolio_history {kode}: scrape selesai, {n} entri di-upsert (terbaru {date_note})"
                        ),
                        row,
                    };
                    crate::redis_cache::set(&kode, &resp).await;
                    (Ok(Response::new(resp)), LogSource::Api, kode)
                }
                Err(e) => (
                    Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                            success: false,
                            message: format!("scrape portofolio history gagal: {e}"),
                            row: None,
                        },
                    )),
                    LogSource::Other,
                    kode,
                ),
            }
        }
        .await;

        let elapsed = started.elapsed().as_millis();
        match log_source {
            LogSource::Cache => eprintln!(
                "\x1b[37mGetPortofolioHistoryByEmitenNameFromStockbit {user_name} {elapsed}ms - HIT FROM CACHE - {log_emiten}\x1b[0m"
            ),
            LogSource::Api => eprintln!(
                "\x1b[32mGetPortofolioHistoryByEmitenNameFromStockbit {user_name} {elapsed}ms - {log_emiten}\x1b[0m"
            ),
            LogSource::Other => Self::log_rpc_debug(
                "GetPortofolioHistoryByEmitenNameFromStockbit",
                &user_name,
                started,
            ),
        }

        result
    }
}
