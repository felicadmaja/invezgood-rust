use std::sync::Arc;

use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::cache::ChartCache;
use crate::pb::chart_server::Chart;
use crate::pb::{GetChartFromInvezgoRequest, GetChartFromInvezgoResponse};

pub struct ChartService {
    cache: Arc<ChartCache>,
    auth_sessions: SessionStore,
}

impl ChartService {
    pub fn new(cache: Arc<ChartCache>, auth_sessions: SessionStore) -> Self {
        Self {
            cache,
            auth_sessions,
        }
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    fn log_rpc_debug(
        rpc_name: &str,
        user_name: &str,
        started: std::time::Instant,
        detail: &str,
    ) {
        eprintln!(
            "{rpc_name} {user_name} {}ms - {detail}",
            started.elapsed().as_millis()
        );
    }

    fn normalize_code(raw: &str) -> Result<String, Status> {
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "code tidak valid ({raw}); wajib tepat 4 huruf alphabet"
            )));
        }
        Ok(code)
    }

    fn normalize_date(field: &str, raw: &str) -> Result<String, Status> {
        let value = raw.trim();
        if value.is_empty() {
            return Err(Status::invalid_argument(format!("{field} wajib diisi")));
        }
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!("{field} harus format YYYY-MM-DD"))
        })?;
        Ok(value.to_string())
    }
}

#[tonic::async_trait]
impl Chart for ChartService {
    async fn get_chart_from_invezgo(
        &self,
        request: Request<GetChartFromInvezgoRequest>,
    ) -> Result<Response<GetChartFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let mut cache_detail = String::new();

        let result: Result<Response<GetChartFromInvezgoResponse>, Status> = async {
            let req = request.into_inner();
            let code = Self::normalize_code(&req.code)?;
            let from_date = Self::normalize_date("from_date", &req.from_date)?;
            let to_date = Self::normalize_date("to_date", &req.to_date)?;

            match self.cache.get_chart(&code, &from_date, &to_date).await {
                Ok((items, detail)) => {
                    cache_detail = detail;
                    Ok(Response::new(GetChartFromInvezgoResponse {
                        success: true,
                        message: format!("{} baris", items.len()),
                        items,
                    }))
                }
                Err(error) => {
                    cache_detail = format!("chart error: {error}");
                    Ok(Response::new(GetChartFromInvezgoResponse {
                        success: false,
                        message: error,
                        items: vec![],
                    }))
                }
            }
        }
        .await;

        Self::log_rpc_debug("GetChartFromInvezgo", &user_name, started, &cache_detail);
        result
    }
}
