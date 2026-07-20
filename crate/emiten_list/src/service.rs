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
    EmitenListRow, GetAllEmitenListRequest, GetAllEmitenListResponse,
    GetEmitenListByCodeNameFromScyllaRequest, GetEmitenListByCodeNameFromScyllaResponse,
    GetEmitenListByCodeNameFromStockbitRequest, GetEmitenListByCodeNameFromStockbitResponse,
    UpdateEmitenListBlueChipRequest, UpdateEmitenListBlueChipResponse,
    UpdateEmitenListCatatanRequest, UpdateEmitenListCatatanResponse,
    UpdateEmitenListFundamentalRequest, UpdateEmitenListFundamentalResponse,
    UpdateEmitenListKonglomerasiRequest, UpdateEmitenListKonglomerasiResponse,
    UpdateEmitenListOwnerRequest, UpdateEmitenListOwnerResponse,
    UpdateEmitenListPlanToTradeRequest, UpdateEmitenListPlanToTradeResponse,
    UpdateEmitenListSectorRequest, UpdateEmitenListSectorResponse,
};

const MAX_EMITEN_SECTOR: i32 = 46;

/// Trim + UPPERCASE; wajib tepat 4 huruf ASCII alphabet (A–Z).
fn parse_code_name(raw: &str) -> Result<String, String> {
    let code = raw.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("code_name wajib tepat 4 huruf alphabet (contoh BBCA)".into());
    }
    Ok(code)
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
    async fn get_all_emiten_list(
        &self,
        request: Request<GetAllEmitenListRequest>,
    ) -> Result<Response<GetAllEmitenListResponse>, Status> {
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
            "GetAllEmitenList {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllEmitenListResponse {
            rows: proto_rows,
        }))
    }

    async fn get_emiten_list_by_code_name_from_scylla(
        &self,
        request: Request<GetEmitenListByCodeNameFromScyllaRequest>,
    ) -> Result<Response<GetEmitenListByCodeNameFromScyllaResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let code_name = parse_code_name(&request.into_inner().code_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_code_name(&code_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("emiten_list code_name={code_name} tidak ditemukan"))
            })?;

        println!(
            "GetEmitenListByCodeNameFromScylla {} {} {}ms",
            username,
            code_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetEmitenListByCodeNameFromScyllaResponse {
            row: Some(row.into_proto()),
        }))
    }

    async fn get_emiten_list_by_code_name_from_stockbit(
        &self,
        request: Request<GetEmitenListByCodeNameFromStockbitRequest>,
    ) -> Result<Response<GetEmitenListByCodeNameFromStockbitResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let code_name = parse_code_name(&request.into_inner().code_name)
            .map_err(Status::invalid_argument)?;

        let mut row: Option<EmitenList> = self
            .repo
            .get_by_code_name(&code_name)
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
                "GetEmitenListByCodeNameFromStockbit {username}: {code_name} {reason} — scrape Stockbit{}...",
                if emiten_missing {
                    " (+ bandarmology)"
                } else {
                    ""
                }
            );
            if let Err(e) = on_demand::scrape_emiten_list_from_stockbit_for_code(
                self.session.clone(),
                &code_name,
                emiten_missing,
            )
            .await
            {
                eprintln!(
                    "GetEmitenListByCodeNameFromStockbit {username}: scrape gagal {code_name}: {e}"
                );
                return Err(Status::internal(format!("scrape Stockbit gagal: {e}")));
            }

            row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;
        }

        let row = row.ok_or_else(|| {
            Status::not_found(format!(
                "emiten_list code_name={code_name} tidak ditemukan setelah scrape"
            ))
        })?;

        println!(
            "GetEmitenListByCodeNameFromStockbit {} {} {}ms",
            username,
            code_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetEmitenListByCodeNameFromStockbitResponse {
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_fundamental_solid(&code_name, req.is_fundamental_solid)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_fundamental_solid={} untuk {code_name} berhasil diupdate",
                    req.is_fundamental_solid
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListFundamental {} {code_name} success={success} {}ms",
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_sector(&code_name, sector)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("sector={sector} untuk {code_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListSector {} {code_name} success={success} {}ms",
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_konglomerasi(&code_name, req.is_konglomerasi)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_konglomerasi={} untuk {code_name} berhasil diupdate",
                    req.is_konglomerasi
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListKonglomerasi {} {code_name} success={success} {}ms",
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_blue_chip(&code_name, req.is_blue_chip)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_blue_chip={} untuk {code_name} berhasil diupdate",
                    req.is_blue_chip
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListBlueChip {} {code_name} success={success} {}ms",
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_plan_to_trade(&code_name, req.is_plan_to_trade)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!(
                    "is_plan_to_trade={} untuk {code_name} berhasil diupdate",
                    req.is_plan_to_trade
                ),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListPlanToTrade {} {code_name} success={success} {}ms",
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

        let code_name = match parse_code_name(&req.code_name) {
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
            .update_catatan(&code_name, &catatan)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("catatan untuk {code_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListCatatan {} {code_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListCatatanResponse {
            success,
            message,
            row,
        }))
    }

    async fn update_emiten_list_owner(
        &self,
        request: Request<UpdateEmitenListOwnerRequest>,
    ) -> Result<Response<UpdateEmitenListOwnerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let code_name = match parse_code_name(&req.code_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(UpdateEmitenListOwnerResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let catatan_owner = req.catatan_owner.trim().to_string();
        let foto_owner: Vec<String> = req
            .foto_owner_gcs_path
            .into_iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        let updated = self
            .repo
            .update_owner(&code_name, &catatan_owner, &foto_owner)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message, row) = if updated {
            let row = self
                .repo
                .get_by_code_name(&code_name)
                .await
                .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
                .map(EmitenList::into_proto);
            (
                true,
                format!("catatan_owner/foto_owner untuk {code_name} berhasil diupdate"),
                row,
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
                None,
            )
        };

        println!(
            "UpdateEmitenListOwner {} {code_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateEmitenListOwnerResponse {
            success,
            message,
            row,
        }))
    }
}
