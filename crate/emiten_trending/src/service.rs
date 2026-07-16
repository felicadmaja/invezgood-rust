use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::emiten_trending_server::EmitenTrending as EmitenTrendingRpc;
use crate::model::EmitenTrending;
use crate::repository::EmitenTrendingRepository;
use crate::{EmitenTrendingRow, GetAllEmitenTrendingRequest, GetAllEmitenTrendingResponse};

pub struct EmitenTrendingService {
    repo: EmitenTrendingRepository,
}

impl EmitenTrendingService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: EmitenTrendingRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl EmitenTrendingRpc for EmitenTrendingService {
    async fn get_all_emiten_trending(
        &self,
        request: Request<GetAllEmitenTrendingRequest>,
    ) -> Result<Response<GetAllEmitenTrendingResponse>, Status> {
        let claims = require_auth(&request)?;
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

        eprintln!(
            "GetAllEmitenTrending oleh user={} email={} date={date_str}",
            claims.name, claims.email
        );

        let rows: Vec<EmitenTrending> = self
            .repo
            .get_all_by_date(date)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenTrendingRow> =
            rows.into_iter().map(EmitenTrending::into_proto).collect();

        Ok(Response::new(GetAllEmitenTrendingResponse {
            rows: proto_rows,
        }))
    }
}
