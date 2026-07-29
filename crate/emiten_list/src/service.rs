use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::on_demand;

use crate::emiten_list_server::EmitenList as EmitenListRpc;
use crate::model::EmitenList;
use crate::repository::EmitenListRepository;
use crate::{
    EmitenListRow, GetAllEmitenListFromScyllaRequest, GetAllEmitenListFromScyllaResponse,
    GetEmitenListByEmitenNameFromScyllaRequest, GetEmitenListByEmitenNameFromScyllaResponse,
    GetEmitenListByEmitenNameFromStockbitRequest, GetEmitenListByEmitenNameFromStockbitResponse,
    GetIdx30FromStockbitRequest, GetIdx30FromStockbitResponse,
    GetIdx80FromStockbitRequest, GetIdx80FromStockbitResponse,
    GetKompas100FromStockbitRequest, GetKompas100FromStockbitResponse,
    GetLq45FromStockbitRequest, GetLq45FromStockbitResponse,
    GetMultiEmitenListFromScyllaRequest, GetMultiEmitenListFromScyllaResponse,
    UpdateEmitenListBlueChipRequest, UpdateEmitenListBlueChipResponse,
    UpdateEmitenListCatatanOwnerRequest, UpdateEmitenListCatatanOwnerResponse,
    UpdateEmitenListCatatanPribadiRequest, UpdateEmitenListCatatanPribadiResponse,
    UpdateEmitenListCatatanRequest, UpdateEmitenListCatatanResponse,
    UpdateEmitenListFundamentalRequest, UpdateEmitenListFundamentalResponse,
    UpdateEmitenListKonglomerasiRequest, UpdateEmitenListKonglomerasiResponse,
    UpdateEmitenListPhotoProfileOwnerRequest, UpdateEmitenListPhotoProfileOwnerResponse,
    UpdateEmitenListPlanToTradeRequest, UpdateEmitenListPlanToTradeResponse,
    UpdateEmitenListSectorRequest, UpdateEmitenListSectorResponse,
    GetTakeProfitWyckoffRequest, GetTakeProfitWyckoffResponse,
    UpdateTakeProfitWyckoffRequest, UpdateTakeProfitWyckoffResponse,
    GetWyckoffPhaseElementRequest, GetWyckoffPhaseElementResponse,
    GetWyckoffTradingRangeRequest, GetWyckoffTradingRangeResponse,
    UpdateWyckoffPhaseElementRequest, UpdateWyckoffPhaseElementResponse,
    UpdateWyckoffTradingRangeRequest, UpdateWyckoffTradingRangeResponse,
};

const MAX_EMITEN_SECTOR: i32 = 47;

/// Trim + UPPERCASE; wajib tepat 4 huruf ASCII alphabet (A–Z).
fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let code = raw.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name wajib tepat 4 huruf alphabet (contoh BBCA)".into());
    }
    Ok(code)
}

/// Normalisasi daftar: UPPERCASE, tepat 4 huruf, unik (urutan tetap).
fn normalize_emiten_names(raw: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = std::collections::HashSet::new();
    for item in raw {
        let Ok(name) = parse_emiten_name(item) else {
            continue;
        };
        if seen.insert(name.clone()) {
            out.push(name);
        }
    }
    out
}

pub struct EmitenListService {
    repo: EmitenListRepository,
    session: Arc<Session>,
}

impl EmitenListService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: EmitenListRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl EmitenListRpc for EmitenListService {
    async fn get_all_emiten_list_from_scylla(
        &self,
        request: Request<GetAllEmitenListFromScyllaRequest>,
    ) -> Result<Response<GetAllEmitenListFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows: Vec<EmitenList> = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> = rows
            .into_iter()
            .map(EmitenList::into_proto)
            .collect();

        println!(
            "GetAllEmitenListFromScylla {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllEmitenListFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_emiten_list_by_emiten_name_from_scylla(
        &self,
        request: Request<GetEmitenListByEmitenNameFromScyllaRequest>,
    ) -> Result<Response<GetEmitenListByEmitenNameFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let emiten_name = parse_emiten_name(&request.into_inner().emiten_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("emiten_list emiten_name={emiten_name} tidak ditemukan"))
            })?;

        println!(
            "GetEmitenListByEmitenNameFromScylla {} {} {}ms",
            username,
            emiten_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetEmitenListByEmitenNameFromScyllaResponse {
            row: Some(row.into_proto()),
        }))
    }

    async fn get_multi_emiten_list_from_scylla(
        &self,
        request: Request<GetMultiEmitenListFromScyllaRequest>,
    ) -> Result<Response<GetMultiEmitenListFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let names = normalize_emiten_names(&req.emiten_name);
        if names.is_empty() {
            return Err(Status::invalid_argument(
                "emiten_name wajib diisi minimal 1 kode valid (4 huruf)",
            ));
        }

        let rows = self
            .repo
            .get_many_by_emiten_names(&names)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> =
            rows.into_iter().map(EmitenList::into_proto).collect();
        println!(
            "GetMultiEmitenListFromScylla {} req={} found={} {}ms",
            username,
            names.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetMultiEmitenListFromScyllaResponse {
            rows: proto_rows,
        }))
    }

    async fn get_emiten_list_by_emiten_name_from_stockbit(
        &self,
        request: Request<GetEmitenListByEmitenNameFromStockbitRequest>,
    ) -> Result<Response<GetEmitenListByEmitenNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let emiten_name = parse_emiten_name(&request.into_inner().emiten_name)
            .map_err(Status::invalid_argument)?;

        let mut row: Option<EmitenList> = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let needs_scrape = match &row {
            None => true,
            Some(r) => {
                worker_scrapping::emiten_list_worker::is_emiten_update_at_stale(r.update_at)
            }
        };

        if needs_scrape {
            let emiten_missing = row.is_none();
            let reason = if emiten_missing {
                "tidak ada"
            } else {
                "update_at stale (≥30 hari)"
            };
            println!(
                "GetEmitenListByEmitenNameFromStockbit {username}: {emiten_name} {reason} — scrape Stockbit{}...",
                if emiten_missing {
                    " (+ bandarmology)"
                } else {
                    ""
                }
            );
            if let Err(e) = on_demand::scrape_emiten_list_from_stockbit_for_code(
                self.session.clone(),
                &emiten_name,
                emiten_missing,
            )
            .await
            {
                let err = e.to_string();
                eprintln!(
                    "GetEmitenListByEmitenNameFromStockbit {username}: scrape gagal {emiten_name}: {err}"
                );
                // HTTP 400 / emiten tidak di BEI → response kosong ke frontend (bukan error gRPC).
                if err.contains(worker_scrapping::emiten_list_worker::EMITEN_NOT_ON_BEI)
                    || err.contains("HTTP 400")
                    || err.contains("400 Bad Request")
                {
                    println!(
                        "GetEmitenListByEmitenNameFromStockbit {} {} empty (HTTP 400 / not on BEI) {}ms",
                        username,
                        emiten_name,
                        started.elapsed().as_millis()
                    );
                    return Ok(Response::new(GetEmitenListByEmitenNameFromStockbitResponse {
                        row: None,
                    }));
                }
                return Err(Status::internal(format!("scrape Stockbit gagal: {err}")));
            }

            row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
        }

        let Some(row) = row else {
            println!(
                "GetEmitenListByEmitenNameFromStockbit {} {} empty (tidak ada di Scylla) {}ms",
                username,
                emiten_name,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetEmitenListByEmitenNameFromStockbitResponse {
                row: None,
            }));
        };

        println!(
            "GetEmitenListByEmitenNameFromStockbit {} {} {}ms",
            username,
            emiten_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetEmitenListByEmitenNameFromStockbitResponse {
            row: Some(row.into_proto()),
        }))
    }

    async fn update_emiten_list_fundamental(
        &self,
        request: Request<UpdateEmitenListFundamentalRequest>,
    ) -> Result<Response<UpdateEmitenListFundamentalResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListFundamentalResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let updated = self
            .repo
            .update_fundamental_solid(&emiten_name, req.is_fundamental_solid)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_fundamental_solid={} untuk {emiten_name} berhasil diupdate",
                    req.is_fundamental_solid
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListFundamental {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListFundamentalResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_sector(
        &self,
        request: Request<UpdateEmitenListSectorRequest>,
    ) -> Result<Response<UpdateEmitenListSectorResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListSectorResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        if req.sector < 0 || req.sector > MAX_EMITEN_SECTOR {
            return Ok(Response::new(UpdateEmitenListSectorResponse {
                success: false,
                message: format!(
                    "sector tidak valid ({}); gunakan nilai EmitenSector 0–{MAX_EMITEN_SECTOR}",
                    req.sector
                ),
                row: None,
            }));
        }

        let sector = req.sector as i8;
        let updated = self
            .repo
            .update_sector(&emiten_name, sector)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = match updated {
            Some(trending_n) => {
                let row = self
                    .repo
                    .get_by_emiten_name(&emiten_name)
                    .await
                    .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                    .map(EmitenList::into_proto);
                (
                    true,
                    format!(
                        "sector={sector} untuk {emiten_name} diupdate \
                         (emiten_list + {trending_n} baris emiten_trending)"
                    ),
                    row,
                )
            }
            None => (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            ),
        };

        println!(
            "UpdateEmitenListSector {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListSectorResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_konglomerasi(
        &self,
        request: Request<UpdateEmitenListKonglomerasiRequest>,
    ) -> Result<Response<UpdateEmitenListKonglomerasiResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListKonglomerasiResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let updated = self
            .repo
            .update_konglomerasi(&emiten_name, req.is_konglomerasi)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_konglomerasi={} untuk {emiten_name} berhasil diupdate",
                    req.is_konglomerasi
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListKonglomerasi {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListKonglomerasiResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_blue_chip(
        &self,
        request: Request<UpdateEmitenListBlueChipRequest>,
    ) -> Result<Response<UpdateEmitenListBlueChipResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListBlueChipResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let updated = self
            .repo
            .update_blue_chip(&emiten_name, req.is_blue_chip)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_blue_chip={} untuk {emiten_name} berhasil diupdate",
                    req.is_blue_chip
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListBlueChip {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListBlueChipResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_plan_to_trade(
        &self,
        request: Request<UpdateEmitenListPlanToTradeRequest>,
    ) -> Result<Response<UpdateEmitenListPlanToTradeResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListPlanToTradeResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let updated = self
            .repo
            .update_plan_to_trade(&emiten_name, req.is_plan_to_trade)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_plan_to_trade={} untuk {emiten_name} berhasil diupdate",
                    req.is_plan_to_trade
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListPlanToTrade {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListPlanToTradeResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_catatan(
        &self,
        request: Request<UpdateEmitenListCatatanRequest>,
    ) -> Result<Response<UpdateEmitenListCatatanResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListCatatanResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let catatan = req.catatan;
        let updated = self
            .repo
            .update_catatan(&emiten_name, &catatan)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("catatan untuk {emiten_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListCatatan {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListCatatanResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_catatan_pribadi(
        &self,
        request: Request<UpdateEmitenListCatatanPribadiRequest>,
    ) -> Result<Response<UpdateEmitenListCatatanPribadiResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListCatatanPribadiResponse {
                    success: false,
                    message,
                }));
            }
        };

        let catatan_pribadi = req.catatan.trim().to_string();
        let updated = self
            .repo
            .update_catatan_pribadi(&emiten_name, &catatan_pribadi)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("catatan_pribadi untuk {emiten_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "UpdateEmitenListCatatanPribadi {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListCatatanPribadiResponse {
            success,
            message,
        }))
    }

    async fn update_emiten_list_catatan_owner(
        &self,
        request: Request<UpdateEmitenListCatatanOwnerRequest>,
    ) -> Result<Response<UpdateEmitenListCatatanOwnerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListCatatanOwnerResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let catatan_owner = req.catatan_owner.trim().to_string();
        let updated = self
            .repo
            .update_catatan_owner(&emiten_name, &catatan_owner)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("catatan_owner untuk {emiten_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListCatatanOwner {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListCatatanOwnerResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_photo_profile_owner(
        &self,
        request: Request<UpdateEmitenListPhotoProfileOwnerRequest>,
    ) -> Result<Response<UpdateEmitenListPhotoProfileOwnerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListPhotoProfileOwnerResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let foto_owner: Vec<String> = req
            .foto_owner_gcs_path
            .into_iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        let updated = self
            .repo
            .update_foto_owner(&emiten_name, &foto_owner)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_emiten_name(&emiten_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("foto_owner untuk {emiten_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListPhotoProfileOwner {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListPhotoProfileOwnerResponse {
            success,
            message,
            row,
        }))
    }

    async fn get_take_profit_wyckoff(
        &self,
        request: Request<GetTakeProfitWyckoffRequest>,
    ) -> Result<Response<GetTakeProfitWyckoffResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let emiten_name = parse_emiten_name(&request.into_inner().emiten_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "emiten_list emiten_name={emiten_name} tidak ditemukan"
                ))
            })?;

        println!(
            "GetTakeProfitWyckoff {} {} {}ms",
            username,
            emiten_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetTakeProfitWyckoffResponse {
            takeprofit_wyckoff: row.takeprofit_wyckoff,
        }))
    }

    async fn get_wyckoff_phase_element(
        &self,
        request: Request<GetWyckoffPhaseElementRequest>,
    ) -> Result<Response<GetWyckoffPhaseElementResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let emiten_name = parse_emiten_name(&request.into_inner().emiten_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "emiten_list emiten_name={emiten_name} tidak ditemukan"
                ))
            })?;

        println!(
            "GetWyckoffPhaseElement {} {} {}ms",
            username,
            emiten_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetWyckoffPhaseElementResponse {
            wyckoff_phase_element: row
                .wyckoff_phase_element
                .into_iter()
                .map(|(k, values)| (k, crate::TextList { values }))
                .collect(),
        }))
    }

    async fn get_wyckoff_trading_range(
        &self,
        request: Request<GetWyckoffTradingRangeRequest>,
    ) -> Result<Response<GetWyckoffTradingRangeResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let emiten_name = parse_emiten_name(&request.into_inner().emiten_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "emiten_list emiten_name={emiten_name} tidak ditemukan"
                ))
            })?;

        println!(
            "GetWyckoffTradingRange {} {} {}ms",
            username,
            emiten_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetWyckoffTradingRangeResponse {
            wyckoff_trading_range: row.wyckoff_trading_range,
        }))
    }

    async fn update_take_profit_wyckoff(
        &self,
        request: Request<UpdateTakeProfitWyckoffRequest>,
    ) -> Result<Response<UpdateTakeProfitWyckoffResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateTakeProfitWyckoffResponse {
                    success: false,
                    message,
                }));
            }
        };

        let takeprofit_wyckoff = std::collections::HashMap::from([
            ("n_kolom".to_string(), req.n_kolom.to_string()),
            (
                "harga_xo_frequent".to_string(),
                req.harga_xo_frequent.to_string(),
            ),
            ("low".to_string(), req.low.to_string()),
            ("takeprofit_1".to_string(), req.takeprofit_1.to_string()),
            ("takeprofit_2".to_string(), req.takeprofit_2.to_string()),
        ]);

        let updated = self
            .repo
            .update_takeprofit_wyckoff(&emiten_name, &takeprofit_wyckoff)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("takeprofit_wyckoff untuk {emiten_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "UpdateTakeProfitWyckoff {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateTakeProfitWyckoffResponse {
            success,
            message,
        }))
    }

    async fn update_wyckoff_phase_element(
        &self,
        request: Request<UpdateWyckoffPhaseElementRequest>,
    ) -> Result<Response<UpdateWyckoffPhaseElementResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateWyckoffPhaseElementResponse {
                    success: false,
                    message,
                }));
            }
        };

        let wyckoff_phase_element: std::collections::HashMap<String, Vec<String>> = req
            .wyckoff_phase_element
            .into_iter()
            .map(|(k, v)| (k, v.values))
            .collect();

        let updated = self
            .repo
            .update_wyckoff_phase_element(&emiten_name, &wyckoff_phase_element)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("wyckoff_phase_element untuk {emiten_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "UpdateWyckoffPhaseElement {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateWyckoffPhaseElementResponse {
            success,
            message,
        }))
    }

    async fn update_wyckoff_trading_range(
        &self,
        request: Request<UpdateWyckoffTradingRangeRequest>,
    ) -> Result<Response<UpdateWyckoffTradingRangeResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateWyckoffTradingRangeResponse {
                    success: false,
                    message,
                }));
            }
        };

        let updated = self
            .repo
            .update_wyckoff_trading_range(&emiten_name, &req.wyckoff_trading_range)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("wyckoff_trading_range untuk {emiten_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "UpdateWyckoffTradingRange {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateWyckoffTradingRangeResponse {
            success,
            message,
        }))
    }

    async fn get_idx30_from_stockbit(
        &self,
        request: Request<GetIdx30FromStockbitRequest>,
    ) -> Result<Response<GetIdx30FromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!("GetIdx30FromStockbit {username}: fetch IDX30 symbols dari Stockbit...");

        let symbols = on_demand::fetch_idx30_symbols_from_stockbit()
            .await
            .map_err(|e| Status::internal(format!("IDX30 Stockbit gagal: {e}")))?;

        if symbols.is_empty() {
            println!(
                "GetIdx30FromStockbit {} symbols=0 rows=0 {}ms",
                username,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetIdx30FromStockbitResponse { rows: vec![] }));
        }

        let rows = self
            .repo
            .get_many_by_emiten_names(&symbols)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> =
            rows.into_iter().map(EmitenList::into_proto).collect();

        println!(
            "GetIdx30FromStockbit {} symbols={} found={} {}ms",
            username,
            symbols.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetIdx30FromStockbitResponse {
            rows: proto_rows,
        }))
    }

    async fn get_lq45_from_stockbit(
        &self,
        request: Request<GetLq45FromStockbitRequest>,
    ) -> Result<Response<GetLq45FromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!("GetLq45FromStockbit {username}: fetch LQ45 symbols dari Stockbit...");

        let symbols = on_demand::fetch_lq45_symbols_from_stockbit()
            .await
            .map_err(|e| Status::internal(format!("LQ45 Stockbit gagal: {e}")))?;

        if symbols.is_empty() {
            println!(
                "GetLq45FromStockbit {} symbols=0 rows=0 {}ms",
                username,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetLq45FromStockbitResponse { rows: vec![] }));
        }

        let rows = self
            .repo
            .get_many_by_emiten_names(&symbols)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> =
            rows.into_iter().map(EmitenList::into_proto).collect();

        println!(
            "GetLq45FromStockbit {} symbols={} found={} {}ms",
            username,
            symbols.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetLq45FromStockbitResponse {
            rows: proto_rows,
        }))
    }

    async fn get_idx80_from_stockbit(
        &self,
        request: Request<GetIdx80FromStockbitRequest>,
    ) -> Result<Response<GetIdx80FromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!("GetIdx80FromStockbit {username}: fetch IDX80 symbols dari Stockbit...");

        let symbols = on_demand::fetch_idx80_symbols_from_stockbit()
            .await
            .map_err(|e| Status::internal(format!("IDX80 Stockbit gagal: {e}")))?;

        if symbols.is_empty() {
            println!(
                "GetIdx80FromStockbit {} symbols=0 rows=0 {}ms",
                username,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetIdx80FromStockbitResponse { rows: vec![] }));
        }

        let rows = self
            .repo
            .get_many_by_emiten_names(&symbols)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> =
            rows.into_iter().map(EmitenList::into_proto).collect();

        println!(
            "GetIdx80FromStockbit {} symbols={} found={} {}ms",
            username,
            symbols.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetIdx80FromStockbitResponse {
            rows: proto_rows,
        }))
    }

    async fn get_kompas100_from_stockbit(
        &self,
        request: Request<GetKompas100FromStockbitRequest>,
    ) -> Result<Response<GetKompas100FromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let _ = request.into_inner();

        println!("GetKompas100FromStockbit {username}: fetch Kompas100 symbols dari Stockbit...");

        let symbols = on_demand::fetch_kompas100_symbols_from_stockbit()
            .await
            .map_err(|e| Status::internal(format!("Kompas100 Stockbit gagal: {e}")))?;

        if symbols.is_empty() {
            println!(
                "GetKompas100FromStockbit {} symbols=0 rows=0 {}ms",
                username,
                started.elapsed().as_millis()
            );
            return Ok(Response::new(GetKompas100FromStockbitResponse {
                rows: vec![],
            }));
        }

        let rows = self
            .repo
            .get_many_by_emiten_names(&symbols)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<EmitenListRow> =
            rows.into_iter().map(EmitenList::into_proto).collect();

        println!(
            "GetKompas100FromStockbit {} symbols={} found={} {}ms",
            username,
            symbols.len(),
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetKompas100FromStockbitResponse {
            rows: proto_rows,
        }))
    }
}
