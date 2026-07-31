use std::sync::{Arc, OnceLock};
use std::time::Instant;

use chrono::{Local, NaiveDate};
use scylla::client::session::Session;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use user::{require_admin, require_auth};
use worker_scrapping::on_demand;

use crate::bandarmology_server::Bandarmology as BandarmologyRpc;
use crate::model::BandarmologyHarian;
use crate::repository::BandarmologyRepository;
use crate::{
    GetBandarmologyHarianFromStockbitRequest, GetBandarmologyHarianFromStockbitResponse,
    GetBrokerAccDistRequest, GetBrokerAccDistResponse,
};

/// Gate: `GetBandarmologyHarianFromStockbit` tidak boleh jalan bersamaan (dua invoke paralel).
/// `Some(rpc_name)` = sedang dipegang.
static HARIAN_RPC_GATE: OnceLock<Mutex<Option<&'static str>>> = OnceLock::new();

fn harian_rpc_gate() -> &'static Mutex<Option<&'static str>> {
    HARIAN_RPC_GATE.get_or_init(|| Mutex::new(None))
}

struct HarianRpcGuard {
    rpc: &'static str,
    guard: tokio::sync::MutexGuard<'static, Option<&'static str>>,
}

impl Drop for HarianRpcGuard {
    fn drop(&mut self) {
        *self.guard = None;
        println!("Bandarmology harian RPC lock dilepas: {}", self.rpc);
    }
}

/// Ambil exclusive lock untuk RPC harian. Gagal segera bila yang lain
/// sedang jalan (tidak mengantri — caller invoke ulang setelah yang aktif selesai).
fn try_acquire_harian_rpc(rpc: &'static str) -> Result<HarianRpcGuard, String> {
    let mut guard = harian_rpc_gate().try_lock().map_err(|_| {
        format!("RPC bandarmology harian lain sedang berjalan; tunggu selesai sebelum invoke {rpc}")
    })?;
    if let Some(holder) = *guard {
        return Err(format!(
            "{holder} sedang berjalan; tunggu selesai sebelum invoke {rpc}"
        ));
    }
    *guard = Some(rpc);
    println!("Bandarmology harian RPC lock diambil: {rpc}");
    Ok(HarianRpcGuard { rpc, guard })
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

    /// Kumpulkan tanggal yang perlu scrape untuk satu emiten (missing / summary kosong).
    /// Hari ini di-skip scrape; baris Scylla hari ini (jika ada) dimasukkan ke `existing`.
    async fn collect_harian_scrape_plan(
        &self,
        kode: &str,
        dates: &[NaiveDate],
        today: NaiveDate,
        username: &str,
        rpc_label: &str,
    ) -> Result<(Vec<NaiveDate>, Vec<BandarmologyHarian>, usize), String> {
        let mut missing = Vec::new();
        let mut existing = Vec::new();
        let mut skipped_today = 0usize;
        for day in dates {
            if *day == today {
                skipped_today += 1;
                if let Ok(Some(row)) = self.repo.find_harian_by_emiten_and_date(kode, *day).await {
                    existing.push(row);
                }
                continue;
            }
            match self.repo.find_harian_by_emiten_and_date(kode, *day).await {
                Ok(Some(row)) if row.needs_scrape_refresh() => {
                    println!(
                        "{rpc_label} {username}: {kode} {day} — \
                         PK ada tapi broker_summary_harian kosong → scrape ulang"
                    );
                    missing.push(*day);
                }
                Ok(Some(row)) => existing.push(row),
                Ok(None) => missing.push(*day),
                Err(e) => {
                    return Err(format!("baca Scylla bandarmology_harian gagal: {e}"));
                }
            }
        }
        Ok((missing, existing, skipped_today))
    }
}

#[tonic::async_trait]
impl BandarmologyRpc for BandarmologyService {
    async fn get_bandarmology_harian_from_stockbit(
        &self,
        request: Request<GetBandarmologyHarianFromStockbitRequest>,
    ) -> Result<Response<GetBandarmologyHarianFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_admin(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let _rpc_lock = match try_acquire_harian_rpc("GetBandarmologyHarianFromStockbit") {
            Ok(g) => g,
            Err(message) => {
                println!(
                    "GetBandarmologyHarianFromStockbit {username}: ditolak (mutual exclusion) — {message}"
                );
                return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                    rows: vec![],
                    success: false,
                    message,
                }));
            }
        };

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
        let (missing, existing_rows, skipped_today) = match self
            .collect_harian_scrape_plan(
                &kode,
                &dates,
                today,
                &username,
                "GetBandarmologyHarianFromStockbit",
            )
            .await
        {
            Ok(v) => v,
            Err(message) => {
                return Ok(Response::new(GetBandarmologyHarianFromStockbitResponse {
                    rows: vec![],
                    success: false,
                    message,
                }));
            }
        };

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
                    "bandarmology_harian {kode}: scrape tidak dijalankan (sudah ada non-empty di Scylla / hari ini){today_note}"
                ),
            }));
        }

        println!(
            "GetBandarmologyHarianFromStockbit {username}: {kode} — scrape {} tanggal (dari {})...",
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
                    "bandarmology_harian {kode}: scrape {n} hari (termasuk refresh summary kosong); \
                     total {}/{} baris di response",
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
                eprintln!("GetBandarmologyHarianFromStockbit {username}: gagal {kode}: {e}");
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

    async fn get_broker_acc_dist(
        &self,
        request: Request<GetBrokerAccDistRequest>,
    ) -> Result<Response<GetBrokerAccDistResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = parse_kode_emiten(&req.emiten_name).map_err(|e| {
            Status::invalid_argument(e.replace("kode_emiten", "emiten_name"))
        })?;
        let dates = parse_tahun_bulan_tanggal_list(&req.tahun_bulan_tanggal);
        if dates.is_empty() {
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal wajib diisi minimal 1 tanggal valid YYYY-MM-DD",
            ));
        }

        let rows = self
            .repo
            .find_many_harian_by_emiten_and_dates(&kode, &dates)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let broker_acc_dist: Vec<_> = rows
            .into_iter()
            .map(BandarmologyHarian::into_proto)
            .collect();
        println!(
            "GetBrokerAccDist {} {} req={} found={} {}ms",
            username,
            kode,
            dates.len(),
            broker_acc_dist.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetBrokerAccDistResponse { broker_acc_dist }))
    }
}
