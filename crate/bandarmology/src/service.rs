use std::sync::Arc;
use std::time::Instant;

use chrono::Local;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::bandarmology_server::Bandarmology as BandarmologyRpc;
use crate::model::{tahun_bulan_from_date, Bandarmology};
use crate::repository::BandarmologyRepository;
use crate::{
    GetBandarmologyFromScyllaRequest, GetBandarmologyFromScyllaResponse,
    GetBandarmologyFromStockbitRequest, GetBandarmologyFromStockbitResponse,
    GetMultiBandarmologyFromScyllaRequest, GetMultiBandarmologyFromScyllaResponse,
};

/// True bila string tepat pola `YYYY-MM` (4 digit-tahun, 2 digit-bulan).
fn is_yyyy_mm(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 7
        && b[4] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
}

fn parse_kode_emiten(raw: &str) -> Result<String, String> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("kode_emiten harus tepat 4 huruf alfabet (contoh: BBCA)".into());
    }
    Ok(kode)
}

fn parse_tahun_bulan(raw: &str) -> Result<String, Status> {
    let tahun_bulan = raw.trim();
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
    Ok(tahun_bulan.to_string())
}

/// Normalisasi daftar kode: UPPERCASE, tepat 4 huruf, unik (urutan tetap).
fn normalize_kode_emitens(raw: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = std::collections::HashSet::new();
    for item in raw {
        let Ok(kode) = parse_kode_emiten(item) else {
            continue;
        };
        if seen.insert(kode.clone()) {
            out.push(kode);
        }
    }
    out
}

pub struct BandarmologyService {
    repo: BandarmologyRepository,
    session: Arc<Session>,
}

impl BandarmologyService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: BandarmologyRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl BandarmologyRpc for BandarmologyService {
    async fn get_bandarmology_from_scylla(
        &self,
        request: Request<GetBandarmologyFromScyllaRequest>,
    ) -> Result<Response<GetBandarmologyFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let tahun_bulan = parse_tahun_bulan(&req.tahun_bulan)?;
        let kode = parse_kode_emiten(&req.kode_emiten).map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .find_by_tahun_bulan_and_emiten(&tahun_bulan, &kode)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "bandarmology tidak ditemukan untuk {tahun_bulan}_{kode}"
                ))
            })?;

        println!(
            "GetBandarmologyFromScylla {} {} {}ms",
            username,
            format!("{tahun_bulan}_{kode}"),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetBandarmologyFromScyllaResponse {
            row: Some(Bandarmology::into_proto(row)),
        }))
    }

    async fn get_multi_bandarmology_from_scylla(
        &self,
        request: Request<GetMultiBandarmologyFromScyllaRequest>,
    ) -> Result<Response<GetMultiBandarmologyFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let tahun_bulan = parse_tahun_bulan(&req.tahun_bulan)?;
        let kodes = normalize_kode_emitens(&req.kode_emiten);
        if kodes.is_empty() {
            return Err(Status::invalid_argument(
                "kode_emiten wajib diisi minimal 1 kode valid (4 huruf)",
            ));
        }

        let rows = self
            .repo
            .find_many_by_tahun_bulan_and_emitens(&tahun_bulan, &kodes)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<_> = rows.into_iter().map(Bandarmology::into_proto).collect();
        println!(
            "GetMultiBandarmologyFromScylla {} {} req={} found={} {}ms",
            username,
            tahun_bulan,
            kodes.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetMultiBandarmologyFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_bandarmology_from_stockbit(
        &self,
        request: Request<GetBandarmologyFromStockbitRequest>,
    ) -> Result<Response<GetBandarmologyFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_kode_emiten(&req.kode_emiten) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(GetBandarmologyFromStockbitResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        println!(
            "GetBandarmologyFromStockbit {username}: {kode} — scrape max 12 bulan + slot minggu w1–w4..."
        );

        match on_demand::scrape_bandarmology_all_the_time_for_code(
            Arc::clone(&self.session),
            &kode,
        )
        .await
        {
            Ok(n) => {
                let cur_tb = tahun_bulan_from_date(Local::now().date_naive());
                let row = self
                    .repo
                    .find_by_tahun_bulan_and_emiten(&cur_tb, &kode)
                    .await
                    .ok()
                    .flatten()
                    .map(Bandarmology::into_proto);
                let weeks = match &row {
                    Some(r) => format!(
                        "w1={} w2={} w3={} w4={}",
                        r.broker_summary_current_w1.is_some(),
                        r.broker_summary_current_w2.is_some(),
                        r.broker_summary_current_w3.is_some(),
                        r.broker_summary_current_w4.is_some(),
                    ),
                    None => "row bulan berjalan belum ada".into(),
                };
                let message = format!(
                    "bandarmology {kode}: scrape selesai, {n} baris bulan di-upsert ({cur_tb}; {weeks})"
                );
                println!(
                    "GetBandarmologyFromStockbit {} {kode} success=true {}ms ({weeks})",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetBandarmologyFromStockbitResponse {
                    success: true,
                    message,
                    row,
                }))
            }
            Err(e) => {
                eprintln!(
                    "GetBandarmologyFromStockbit {username}: gagal {kode}: {e}"
                );
                println!(
                    "GetBandarmologyFromStockbit {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetBandarmologyFromStockbitResponse {
                    success: false,
                    message: format!("scrape bandarmology gagal: {e}"),
                    row: None,
                }))
            }
        }
    }
}
