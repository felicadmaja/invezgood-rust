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
    GetEmitenListByCodeNameRequest, GetEmitenListByCodeNameResponse,
    UpdateEmitenListBlueChipRequest, UpdateEmitenListBlueChipResponse,
    UpdateEmitenListCatatanRequest, UpdateEmitenListCatatanResponse,
    UpdateEmitenListFundamentalRequest, UpdateEmitenListFundamentalResponse,
    UpdateEmitenListKonglomerasiRequest, UpdateEmitenListKonglomerasiResponse,
    UpdateEmitenListOwnerRequest, UpdateEmitenListOwnerResponse,
    UpdateEmitenListSectorRequest, UpdateEmitenListSectorResponse,
};

const MAX_EMITEN_SECTOR: i32 = 46;

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

    async fn get_emiten_list_by_code_name(
        &self,
        request: Request<GetEmitenListByCodeNameRequest>,
    ) -> Result<Response<GetEmitenListByCodeNameResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let code_name = request.into_inner().code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Err(Status::invalid_argument("code_name wajib diisi"));
        }

        let mut row: Option<EmitenList> = self
            .repo
            .get_by_code_name(&code_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        if row.is_none() {
            println!(
                "GetEmitenListByCodeName {username}: {code_name} tidak ada — on-demand scrape..."
            );
            if let Err(e) =
                on_demand::ensure_emiten_data_for_code(self.session.clone(), &code_name).await
            {
                eprintln!("GetEmitenListByCodeName {username}: on-demand gagal {code_name}: {e}");
                return Err(Status::internal(format!("on-demand scrape gagal: {e}")));
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
            "GetEmitenListByCodeName {} {} {}ms",
            username,
            code_name,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetEmitenListByCodeNameResponse {
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListFundamentalResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        let updated = self
            .repo
            .update_fundamental_solid(&code_name, req.is_fundamental_solid)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!(
                    "is_fundamental_solid={} untuk {code_name} berhasil diupdate",
                    req.is_fundamental_solid
                ),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListSectorResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        if req.sector < 0 || req.sector > MAX_EMITEN_SECTOR {
            return Ok(Response::new(UpdateEmitenListSectorResponse {
                success: false,
                message: format!(
                    "sector tidak valid ({}); gunakan nilai EmitenSector 0–{MAX_EMITEN_SECTOR}",
                    req.sector
                ),
            }));
        }

        let sector = req.sector as i8;
        let updated = self
            .repo
            .update_sector(&code_name, sector)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("sector={sector} untuk {code_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListKonglomerasiResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        let updated = self
            .repo
            .update_konglomerasi(&code_name, req.is_konglomerasi)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!(
                    "is_konglomerasi={} untuk {code_name} berhasil diupdate",
                    req.is_konglomerasi
                ),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListBlueChipResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        let updated = self
            .repo
            .update_blue_chip(&code_name, req.is_blue_chip)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!(
                    "is_blue_chip={} untuk {code_name} berhasil diupdate",
                    req.is_blue_chip
                ),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListCatatanResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        let catatan = req.catatan;
        let updated = self
            .repo
            .update_catatan(&code_name, &catatan)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("catatan untuk {code_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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

        let code_name = req.code_name.trim().to_ascii_uppercase();
        if code_name.is_empty() {
            return Ok(Response::new(UpdateEmitenListOwnerResponse {
                success: false,
                message: "code_name wajib diisi".to_string(),
            }));
        }

        let catatan_owner = req.catatan_owner.trim().to_string();
        let foto_owner = req.foto_owner_gcs_path.trim().to_string();
        let updated = self
            .repo
            .update_owner(&code_name, &catatan_owner, &foto_owner)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("catatan_owner/foto_owner untuk {code_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("emiten_list code_name={code_name} tidak ditemukan"),
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
        }))
    }
}
