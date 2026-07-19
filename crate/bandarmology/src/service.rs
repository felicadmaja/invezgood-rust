use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::bandarmology_server::Bandarmology as BandarmologyRpc;
use crate::model::Bandarmology;
use crate::repository::BandarmologyRepository;
use crate::{GetBandarmologyRequest, GetBandarmologyResponse};

/// True bila string tepat pola `YYYY-MM` (4 digit-tahun, 2 digit-bulan).
fn is_yyyy_mm(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 7
        && b[4] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
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

        let tahun_bulan = req.tahun_bulan.trim();
        if !is_yyyy_mm(tahun_bulan) {
            return Err(Status::invalid_argument(
                "tahun_bulan harus format YYYY-MM (contoh: 2026-07)",
            ));
        }

        let month = tahun_bulan[5..7].parse::<u8>().map_err(|_| {
            Status::invalid_argument("tahun_bulan harus format YYYY-MM (contoh: 2026-07)")
        })?;
        if !(1..=12).contains(&month) {
            return Err(Status::invalid_argument(
                "tahun_bulan harus bulan valid 01–12 (contoh: 2026-07)",
            ));
        }

        let kode = req.kode_emiten.trim().to_ascii_uppercase();
        if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(
                "kode_emiten harus tepat 4 huruf alfabet (contoh: BBCA)",
            ));
        }

        let row = self
            .repo
            .find_by_tahun_bulan_and_emiten(tahun_bulan, &kode)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "bandarmology tidak ditemukan untuk {tahun_bulan}_{kode}"
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
