use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{
    extract_bearer_token, validate_session, AuthSession,
    SessionStore,
};
use worker_scrapping::on_demand;

use crate::model::PortofolioRow as DbPortofolioRow;
use portofolio_equity::repository::PortofolioEquityRepository;
use crate::pb::portofolio_server::Portofolio;
use crate::pb::{
    GetAllPortofolioFromScyllaRequest, GetAllPortofolioFromScyllaResponse,
    GetAllPortofolioFromStockbitRequest, GetAllPortofolioFromStockbitResponse,
    GetPortofolioFromScyllaByEmitenNameRequest, GetPortofolioFromScyllaByEmitenNameResponse,
    PortofolioEquityRow, PortofolioRow,
};

const PORTFOLIO_SCRAPE_COOLDOWN: Duration = Duration::from_secs(3 * 60);

static LAST_PORTFOLIO_SCRAPE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn portfolio_scrape_gate() -> &'static Mutex<Option<Instant>> {
    LAST_PORTFOLIO_SCRAPE.get_or_init(|| Mutex::new(None))
}

async fn acquire_portfolio_scrape_slot() -> Result<(), Status> {
    let mut last = portfolio_scrape_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < PORTFOLIO_SCRAPE_COOLDOWN {
            let remaining_secs = (PORTFOLIO_SCRAPE_COOLDOWN - elapsed).as_secs().max(1);
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 3 menit untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(Instant::now());
    Ok(())
}

#[derive(Clone)]
pub struct PortofolioService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
    equity_repo: PortofolioEquityRepository,
}

impl PortofolioService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        let equity_repo = PortofolioEquityRepository::new(session.clone());
        Self {
            session,
            auth_sessions,
            equity_repo,
        }
    }

    async fn require_admin<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))?;
        if auth.role.trim().eq_ignore_ascii_case("admin") {
            Ok(auth)
        } else {
            Err(Status::permission_denied("Harus admin !"))
        }
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }

    fn normalize_emiten_name(raw: &str) -> Result<String, Status> {
        let name = raw.trim().to_ascii_uppercase();
        if name.len() != 4 || !name.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "emiten_name tidak valid ({raw}); wajib tepat 4 huruf alphabet"
            )));
        }
        Ok(name)
    }

    fn row_to_proto(row: DbPortofolioRow) -> PortofolioRow {
        PortofolioRow {
            emiten_name: row.emiten_name,
            emiten_icon: row.emiten_icon,
            balance_lot: row.balance_lot,
            available_lot: row.available_lot,
            average_price: row.average_price,
            current_price: row.current_price,
            invested: row.invested,
            market_value: row.market_value,
            potential_p_l: row.potential_p_l,
            percentage: row.percentage,
            long_name: row.long_name,
        }
    }

    async fn load_equity_proto_rows(&self) -> Result<Vec<PortofolioEquityRow>, Status> {
        let rows = self
            .equity_repo
            .get_all()
            .await
            .map_err(Status::internal)?;
        Ok(rows
            .into_iter()
            .map(|row| PortofolioEquityRow {
                nama: row.nama,
                value: row.value,
            })
            .collect())
    }
}

#[tonic::async_trait]
impl Portofolio for PortofolioService {
    async fn get_all_portofolio_from_scylla(
        &self,
        request: Request<GetAllPortofolioFromScyllaRequest>,
    ) -> Result<Response<GetAllPortofolioFromScyllaResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllPortofolioFromScyllaResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = crate::repository::find_all(self.session.as_ref())
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetAllPortofolioFromScyllaResponse {
                rows: rows.into_iter().map(Self::row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllPortofolioFromScylla", &user_name, started);
        result
    }

    async fn get_portofolio_from_scylla_by_emiten_name(
        &self,
        request: Request<GetPortofolioFromScyllaByEmitenNameRequest>,
    ) -> Result<Response<GetPortofolioFromScyllaByEmitenNameResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;
        let raw_emiten = request.into_inner().emiten_name;
        let emiten_log = raw_emiten.trim().to_ascii_uppercase();

        let result: Result<Response<GetPortofolioFromScyllaByEmitenNameResponse>, Status> = async {
            let emiten_name = Self::normalize_emiten_name(&raw_emiten)?;
            let row = crate::repository::find_by_emiten_name(self.session.as_ref(), &emiten_name)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetPortofolioFromScyllaByEmitenNameResponse {
                row: row.map(Self::row_to_proto),
            }))
        }
        .await;

        eprintln!(
            "GetPortofolioFromScyllaByEmitenName {user_name} {emiten_log} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn get_all_portofolio_from_stockbit(
        &self,
        request: Request<GetAllPortofolioFromStockbitRequest>,
    ) -> Result<Response<GetAllPortofolioFromStockbitResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_admin(&request).await?;
        let user_name = auth.nama;
        let _ = request.into_inner();

        let result: Result<Response<GetAllPortofolioFromStockbitResponse>, Status> = async {
            acquire_portfolio_scrape_slot().await?;

            match on_demand::scrape_portofolio_all(Arc::clone(&self.session)).await {
                Ok((n, _)) => {
                    let rows = crate::repository::find_all(self.session.as_ref())
                        .await
                        .map_err(Status::internal)?;
                    let proto_rows: Vec<PortofolioRow> =
                        rows.into_iter().map(Self::row_to_proto).collect();
                    let equity_rows = self.load_equity_proto_rows().await?;
                    Ok(Response::new(GetAllPortofolioFromStockbitResponse {
                        success: true,
                        message: format!(
                            "portofolio: scrape selesai, {n} holdings di-upsert (baca {} holdings, {} equity)",
                            proto_rows.len(),
                            equity_rows.len()
                        ),
                        rows: proto_rows,
                        equity_rows,
                    }))
                }
                Err(e) => Ok(Response::new(GetAllPortofolioFromStockbitResponse {
                    success: false,
                    message: format!("scrape portofolio gagal: {e}"),
                    rows: vec![],
                    equity_rows: vec![],
                })),
            }
        }
        .await;

        Self::log_rpc_debug("GetAllPortofolioFromStockbit", &user_name, started);
        result
    }
}
