use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDate;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::bandarmology_server::Bandarmology as BandarmologyRpc;
use crate::model::Bandarmology;
use crate::repository::BandarmologyRepository;
use crate::{GetBandarmologyRequest, GetBandarmologyResponse};

pub struct BandarmologyService {
    repo: BandarmologyRepository,
}

impl BandarmologyService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: BandarmologyRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl BandarmologyRpc for BandarmologyService {
    async fn get_bandarmology(
        &self,
        request: Request<GetBandarmologyRequest>,
    ) -> Result<Response<GetBandarmologyResponse>, Status> {
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

        let kode = req
            .kode_emiten
            .trim()
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_uppercase())
            .collect::<String>();
        if kode.is_empty() {
            return Err(Status::invalid_argument("kode_emiten wajib diisi"));
        }

        let row = self
            .repo
            .find_by_date_and_emiten(date, &kode)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "bandarmology tidak ditemukan untuk {date_str}_{kode}"
                ))
            })?;

        println!(
            "GetBandarmology {} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetBandarmologyResponse {
            row: Some(Bandarmology::into_proto(row)),
        }))
    }
}
