use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::PortofolioCatatan;
use crate::portofolio_catatan_server::PortofolioCatatan as PortofolioCatatanRpc;
use crate::repository::PortofolioCatatanRepository;
use crate::{
    DeletePortofolioCatatanRequest, DeletePortofolioCatatanResponse,
    GetAllPortofolioCatatanRequest, GetAllPortofolioCatatanResponse,
    GetPortofolioCatatanByEmitenNameRequest, GetPortofolioCatatanByEmitenNameResponse,
    InsertPortofolioCatatanRequest, InsertPortofolioCatatanResponse, PortofolioCatatanRow,
    UpdatePortofolioCatatanRequest, UpdatePortofolioCatatanResponse,
};

pub struct PortofolioCatatanService {
    repo: PortofolioCatatanRepository,
}

impl PortofolioCatatanService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: PortofolioCatatanRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

/// Normalisasi kode emiten: trim, UPPERCASE, wajib tepat 4 huruf alphabet.
fn normalize_emiten_name(raw: &str) -> Result<String, String> {
    let name = raw.trim().to_ascii_uppercase();
    if name.len() != 4 || !name.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!(
            "emiten_name tidak valid ({raw}); wajib tepat 4 huruf alphabet"
        ));
    }
    Ok(name)
}

#[tonic::async_trait]
impl PortofolioCatatanRpc for PortofolioCatatanService {
    async fn insert_portofolio_catatan(
        &self,
        request: Request<InsertPortofolioCatatanRequest>,
    ) -> Result<Response<InsertPortofolioCatatanResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match normalize_emiten_name(&req.emiten_name) {
            Ok(name) => name,
            Err(message) => {
                return Ok(Response::new(InsertPortofolioCatatanResponse {
                    success: false,
                    message,
                }));
            }
        };
        let catatan = req.catatan.trim().to_string();

        let inserted = self
            .repo
            .insert(&emiten_name, &catatan)
            .await
            .map_err(|e| Status::internal(format!("Scylla insert failed: {e}")))?;

        let (success, message) = if inserted {
            (
                true,
                format!("portofolio_catatan emiten_name={emiten_name} berhasil diinsert"),
            )
        } else {
            (
                false,
                format!("portofolio_catatan emiten_name={emiten_name} sudah ada"),
            )
        };

        println!(
            "InsertPortofolioCatatan {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(InsertPortofolioCatatanResponse {
            success,
            message,
        }))
    }

    async fn get_all_portofolio_catatan(
        &self,
        request: Request<GetAllPortofolioCatatanRequest>,
    ) -> Result<Response<GetAllPortofolioCatatanResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<PortofolioCatatanRow> =
            rows.into_iter().map(PortofolioCatatan::into_proto).collect();

        println!(
            "GetAllPortofolioCatatan {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllPortofolioCatatanResponse {
            rows: proto_rows,
        }))
    }

    async fn get_portofolio_catatan_by_emiten_name(
        &self,
        request: Request<GetPortofolioCatatanByEmitenNameRequest>,
    ) -> Result<Response<GetPortofolioCatatanByEmitenNameResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = normalize_emiten_name(&req.emiten_name)
            .map_err(Status::invalid_argument)?;

        let row = self
            .repo
            .get_by_emiten_name(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        println!(
            "GetPortofolioCatatanByEmitenName {} {emiten_name} found={} {}ms",
            username,
            row.is_some(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetPortofolioCatatanByEmitenNameResponse {
            row: row.map(PortofolioCatatan::into_proto),
        }))
    }

    async fn update_portofolio_catatan(
        &self,
        request: Request<UpdatePortofolioCatatanRequest>,
    ) -> Result<Response<UpdatePortofolioCatatanResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match normalize_emiten_name(&req.emiten_name) {
            Ok(name) => name,
            Err(message) => {
                return Ok(Response::new(UpdatePortofolioCatatanResponse {
                    success: false,
                    message,
                }));
            }
        };
        let catatan = req.catatan.trim().to_string();

        let updated = self
            .repo
            .update(&emiten_name, &catatan)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (
                true,
                format!("portofolio_catatan emiten_name={emiten_name} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("portofolio_catatan emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "UpdatePortofolioCatatan {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdatePortofolioCatatanResponse {
            success,
            message,
        }))
    }

    async fn delete_portofolio_catatan(
        &self,
        request: Request<DeletePortofolioCatatanRequest>,
    ) -> Result<Response<DeletePortofolioCatatanResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let emiten_name = match normalize_emiten_name(&req.emiten_name) {
            Ok(name) => name,
            Err(message) => {
                return Ok(Response::new(DeletePortofolioCatatanResponse {
                    success: false,
                    message,
                }));
            }
        };

        let deleted = self
            .repo
            .delete(&emiten_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla delete failed: {e}")))?;

        let (success, message) = if deleted {
            (
                true,
                format!("portofolio_catatan emiten_name={emiten_name} berhasil dihapus"),
            )
        } else {
            (
                false,
                format!("portofolio_catatan emiten_name={emiten_name} tidak ditemukan"),
            )
        };

        println!(
            "DeletePortofolioCatatan {} {emiten_name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(DeletePortofolioCatatanResponse {
            success,
            message,
        }))
    }
}
