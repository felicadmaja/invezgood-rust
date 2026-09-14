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

    async fn require_admin<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let auth = self.require_auth(request).await?;
        if auth.role.trim().eq_ignore_ascii_case("admin") {
            Ok(auth)
        } else {
            Err(Status::permission_denied("hanya role admin"))
        }
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }

    async fn read_emiten_from_scylla_for_stockbit(
        &self,
        kode: &str,
        source_note: &str,
    ) -> GetPortofolioHistoryByEmitenNameFromStockbitResponse {
        match self.repo.find_all_by_emiten(kode).await {
            Ok(rows) if rows.is_empty() => GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                success: false,
                message: format!("portofolio_history {kode}: tidak ada di Scylla"),
                rows: vec![],
            },
            Ok(rows) => {
                let n_entri: usize = rows.iter().map(|r| r.history.len()).sum();
                GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                    success: true,
                    message: format!(
                        "portofolio_history {kode}: {n_entri} entri dari Scylla ({} tanggal){source_note}",
                        rows.len()
                    ),
                    rows: rows.into_iter().map(|r| r.into_proto()).collect(),
                }
            }
            Err(e) => GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                success: false,
                message: format!("baca portofolio_history gagal: {e}"),
                rows: vec![],
            },
        }
    }
}

#[tonic::async_trait]
impl PortofolioHistoryRpc for PortofolioHistoryService {
    async fn get_portofolio_history_by_emiten_name_from_scylla(
        &self,
        request: Request<GetPortofolioHistoryByEmitenNameFromScyllaRequest>,
    ) -> Result<Response<GetPortofolioHistoryByEmitenNameFromScyllaResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_admin(&request).await?;
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
                                rows: vec![],
                            },
                        ));
                    }
                };

                match self.repo.find_all_by_emiten(&kode).await {
                    Ok(rows) if rows.is_empty() => Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                            success: false,
                            message: format!("portofolio_history {kode}: tidak ada di Scylla"),
                            rows: vec![],
                        },
                    )),
                    Ok(rows) => {
                        let n_entri: usize = rows.iter().map(|r| r.history.len()).sum();
                        Ok(Response::new(
                            GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                                success: true,
                                message: format!(
                                    "portofolio_history {kode}: {n_entri} entri dari Scylla ({} tanggal)",
                                    rows.len()
                                ),
                                rows: rows.into_iter().map(|r| r.into_proto()).collect(),
                            },
                        ))
                    }
                    Err(e) => Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromScyllaResponse {
                            success: false,
                            message: format!("baca portofolio_history gagal: {e}"),
                            rows: vec![],
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
        let auth = self.require_admin(&request).await?;
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
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;
        let emiten_invoke = request.get_ref().emiten_name.trim().to_string();
        eprintln!(
            "GetPortofolioHistoryByEmitenNameFromStockbit invoke {user_name} emiten={emiten_invoke}"
        );

        enum LogSource {
            Moka,
            Scrape,
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
                                rows: vec![],
                            },
                        )),
                        LogSource::Other,
                        String::new(),
                    );
                }
            };

            if crate::stockbit_cache::is_fresh(&kode).await {
                let resp = self
                    .read_emiten_from_scylla_for_stockbit(&kode, " (moka ≤15m)")
                    .await;
                return (Ok(Response::new(resp)), LogSource::Moka, kode);
            }

            if let Err(status) = acquire_history_scrape_slot().await {
                return (Err(status), LogSource::Other, kode);
            }

            if let Err(e) =
                on_demand::scrape_portofolio_history_for_emiten(Arc::clone(&self.session), &kode)
                    .await
            {
                return (
                    Ok(Response::new(
                        GetPortofolioHistoryByEmitenNameFromStockbitResponse {
                            success: false,
                            message: format!("scrape portofolio history gagal: {e}"),
                            rows: vec![],
                        },
                    )),
                    LogSource::Other,
                    kode,
                );
            }

            crate::stockbit_cache::mark_scraped(&kode).await;
            let resp = self
                .read_emiten_from_scylla_for_stockbit(&kode, "")
                .await;
            (Ok(Response::new(resp)), LogSource::Scrape, kode)
        }
        .await;

        let elapsed = started.elapsed().as_millis();
        match log_source {
            LogSource::Moka => eprintln!(
                "\x1b[37mGetPortofolioHistoryByEmitenNameFromStockbit {user_name} {elapsed}ms - HIT moka - {log_emiten}\x1b[0m"
            ),
            LogSource::Scrape => eprintln!(
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
