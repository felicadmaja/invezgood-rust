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

/// True bila string tepat pola `YYYY-MM-DD` (4 digit-tahun, 2 digit-bulan, 2 digit-hari).
fn is_yyyy_mm_dd(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

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
        if !is_yyyy_mm_dd(date_str) {
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal harus format YYYY-MM-DD (contoh: 2026-07-16)",
            ));
        }
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(
                "tahun_bulan_tanggal harus tanggal valid format YYYY-MM-DD (contoh: 2026-07-16)",
            )
        })?;

        let kode = req.kode_emiten.trim().to_ascii_uppercase();
        if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(
                "kode_emiten harus tepat 4 huruf alfabet (contoh: BBCA)",
            ));
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
