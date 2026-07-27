use std::sync::Arc;
use std::time::Instant;

use chrono::{Local, NaiveDate};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::bandarmology_server::Bandarmology as BandarmologyRpc;
use crate::model::{Bandarmology, BandarmologyHarian};
use crate::repository::BandarmologyRepository;
use crate::{
    GetBandarmologyFromScyllaRequest, GetBandarmologyFromScyllaResponse,
    GetBandarmologyHarianFromStockbitRequest, GetBandarmologyHarianFromStockbitResponse,
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

/// Parse daftar `YYYY-MM-DD`; duplikat di-skip; format invalid diabaikan (urut naik).
fn parse_tahun_bulan_tanggal_list(raw: &[String]) -> Vec<NaiveDate> {
    let mut out = Vec::with_capacity(raw.len());
    for s in raw {
        let t = s.trim();
        if let Ok(d) = NaiveDate::parse_from_str(t, "%Y-%m-%d") {
            out.push(d);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
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

    async fn get_bandarmology_harian_from_stockbit(
        &self,
        request: Request<GetBandarmologyHarianFromStockbitRequest>,
    ) -> Result<Response<GetBandarmologyHarianFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_kode_emiten(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                    rows: vec![],
                    success: false,
                    message,
                }));
            }
        };

        let dates = parse_tahun_bulan_tanggal_list(&req.tahun_bulan_tanggal);
        if dates.is_empty() {
            return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                rows: vec![],
                success: false,
                message: "tahun_bulan_tanggal kosong / tidak ada tanggal valid YYYY-MM-DD".into(),
            }));
        }

        let today = Local::now().date_naive();
        let mut missing = Vec::new();
        let mut existing_rows = Vec::new();
        let mut skipped_today = 0usize;
        for day in &dates {
            if *day == today {
                skipped_today += 1;
                // Hari ini: jangan scrape; tetap kembalikan baris Scylla bila sudah ada.
                if let Ok(Some(row)) = self.repo.find_harian_by_emiten_and_date(&kode, *day).await {
                    existing_rows.push(row);
                }
                continue;
            }
            match self.repo.find_harian_by_emiten_and_date(&kode, *day).await {
                Ok(Some(row)) => existing_rows.push(row),
                Ok(None) => missing.push(*day),
                Err(e) => {
                    return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                        rows: existing_rows
                            .into_iter()
                            .map(BandarmologyHarian::into_proto)
                            .collect(),
                        success: false,
                        message: format!("baca Scylla bandarmology_harian gagal: {e}"),
                    }));
                }
            }
        }

        if missing.is_empty() {
            let today_note = if skipped_today > 0 {
                format!("; {skipped_today} tanggal hari ini ({today}) di-skip scrape")
            } else {
                String::new()
            };
            println!(
                "GetBandarmologyHarianFromStockbit {} {kode}: tidak ada tanggal yang perlu scrape{} — {}ms",
                username,
                today_note,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                rows: existing_rows
                    .into_iter()
                    .map(BandarmologyHarian::into_proto)
                    .collect(),
                success: true,
                message: format!(
                    "bandarmology_harian {kode}: scrape tidak dijalankan (sudah ada di Scylla / hari ini){today_note}"
                ),
            }));
        }

        println!(
            "GetBandarmologyHarianFromStockbit {username}: {kode} — scrape {} tanggal missing (dari {})...",
            missing.len(),
            dates.len()
        );

        match on_demand::scrape_bandarmology_harian_days_from_stockbit(
            Arc::clone(&self.session),
            &kode,
            &missing,
        )
        .await
        {
            Ok(n) => {
                let mut rows = Vec::with_capacity(dates.len());
                for day in &dates {
                    if let Ok(Some(row)) =
                        self.repo.find_harian_by_emiten_and_date(&kode, *day).await
                    {
                        rows.push(row.into_proto());
                    }
                }
                let message = format!(
                    "bandarmology_harian {kode}: scrape {n} hari baru; total {}/{} baris di response",
                    rows.len(),
                    dates.len()
                );
                println!(
                    "GetBandarmologyHarianFromStockbit {} {kode} success=true {}ms ({message})",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                    rows,
                    success: true,
                    message,
                }))
            }
            Err(e) => {
                let rows: Vec<_> = existing_rows
                    .into_iter()
                    .map(BandarmologyHarian::into_proto)
                    .collect();
                eprintln!(
                    "GetBandarmologyHarianFromStockbit {username}: gagal {kode}: {e}"
                );
                println!(
                    "GetBandarmologyHarianFromStockbit {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                    rows,
                    success: false,
                    message: format!("scrape bandarmology_harian gagal: {e}"),
                }))
            }
        }
    }
}
