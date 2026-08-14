use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{jenis_from_proto, phase_from_proto, WyckoffGlossary};
use crate::repository::WyckoffGlossaryRepository;
use crate::wyckoff_glossary_server::WyckoffGlossary as WyckoffGlossaryRpc;
use crate::{
    DeleteWyckoffGlossaryRequest, DeleteWyckoffGlossaryResponse, GetAllWyckoffGlossaryRequest,
    GetAllWyckoffGlossaryResponse, InsertWyckoffGlossaryRequest, InsertWyckoffGlossaryResponse,
    UpdateWyckoffGlossaryRequest, UpdateWyckoffGlossaryResponse, WyckoffGlossaryRow,
};

pub struct WyckoffGlossaryService {
    repo: WyckoffGlossaryRepository,
    auth_sessions: SessionStore,
}

impl WyckoffGlossaryService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            repo: WyckoffGlossaryRepository::new(session),
            auth_sessions,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }
}

#[tonic::async_trait]
impl WyckoffGlossaryRpc for WyckoffGlossaryService {
    async fn get_all_wyckoff_glossary(
        &self,
        request: Request<GetAllWyckoffGlossaryRequest>,
    ) -> Result<Response<GetAllWyckoffGlossaryResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let username = auth.nama;

        let rows = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<WyckoffGlossaryRow> =
            rows.into_iter().map(WyckoffGlossary::into_proto).collect();

        eprintln!(
            "GetAllWyckoffGlossary {username} rows={} {}ms",
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllWyckoffGlossaryResponse {
            rows: proto_rows,
        }))
    }

    async fn insert_wyckoff_glossary(
        &self,
        request: Request<InsertWyckoffGlossaryRequest>,
    ) -> Result<Response<InsertWyckoffGlossaryResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let username = auth.nama;
        let req = request.into_inner();

        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Ok(Response::new(InsertWyckoffGlossaryResponse {
                success: false,
                message: "name wajib diisi".to_string(),
            }));
        }

        let long_name = req.long_name.trim().to_string();
        let description = req.description.trim().to_string();
        let jenis = jenis_from_proto(req.jenis);
        let phase = phase_from_proto(req.phase);
        let urutan_tampil = if req.urutan_tampil == 0 {
            None
        } else {
            Some(req.urutan_tampil)
        };

        let inserted = self
            .repo
            .insert(&name, &long_name, &description, urutan_tampil, &jenis, &phase)
            .await
            .map_err(|e| Status::internal(format!("Scylla insert failed: {e}")))?;

        let (success, message) = if inserted {
            (true, format!("wyckoff_glossary name={name} berhasil diinsert"))
        } else {
            (false, format!("wyckoff_glossary name={name} sudah ada"))
        };

        eprintln!(
            "InsertWyckoffGlossary {username} {name} success={success} {}ms",
            started.elapsed().as_millis()
        );

        Ok(Response::new(InsertWyckoffGlossaryResponse {
            success,
            message,
        }))
    }

    async fn update_wyckoff_glossary(
        &self,
        request: Request<UpdateWyckoffGlossaryRequest>,
    ) -> Result<Response<UpdateWyckoffGlossaryResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let username = auth.nama;
        let req = request.into_inner();

        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Ok(Response::new(UpdateWyckoffGlossaryResponse {
                success: false,
                message: "name wajib diisi".to_string(),
            }));
        }

        let long_name = req.long_name.trim().to_string();
        let description = req.description.trim().to_string();
        let jenis = jenis_from_proto(req.jenis);
        let phase = phase_from_proto(req.phase);
        let urutan_tampil = if req.urutan_tampil == 0 {
            None
        } else {
            Some(req.urutan_tampil)
        };

        let updated = self
            .repo
            .update(&name, &long_name, &description, urutan_tampil, &jenis, &phase)
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let (success, message) = if updated {
            (true, format!("wyckoff_glossary name={name} berhasil diupdate"))
        } else {
            (false, format!("wyckoff_glossary name={name} tidak ditemukan"))
        };

        eprintln!(
            "UpdateWyckoffGlossary {username} {name} success={success} {}ms",
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateWyckoffGlossaryResponse {
            success,
            message,
        }))
    }

    async fn delete_wyckoff_glossary(
        &self,
        request: Request<DeleteWyckoffGlossaryRequest>,
    ) -> Result<Response<DeleteWyckoffGlossaryResponse>, Status> {
        let started = Instant::now();
        let auth = self.require_auth(&request).await?;
        let username = auth.nama;

        let name = request.into_inner().name.trim().to_string();
        if name.is_empty() {
            return Ok(Response::new(DeleteWyckoffGlossaryResponse {
                success: false,
                message: "name wajib diisi".to_string(),
            }));
        }

        let deleted = self
            .repo
            .delete(&name)
            .await
            .map_err(|e| Status::internal(format!("Scylla delete failed: {e}")))?;

        let (success, message) = if deleted {
            (true, format!("wyckoff_glossary name={name} berhasil dihapus"))
        } else {
            (false, format!("wyckoff_glossary name={name} tidak ditemukan"))
        };

        eprintln!(
            "DeleteWyckoffGlossary {username} {name} success={success} {}ms",
            started.elapsed().as_millis()
        );

        Ok(Response::new(DeleteWyckoffGlossaryResponse {
            success,
            message,
        }))
    }
}
