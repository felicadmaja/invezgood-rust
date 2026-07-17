use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::emiten_list_server::EmitenList as EmitenListRpc;
use crate::model::EmitenList;
use crate::repository::EmitenListRepository;
use crate::{
    EmitenListRow, GetAllEmitenListRequest, GetAllEmitenListResponse,
    GetEmitenListByCodeNameRequest, GetEmitenListByCodeNameResponse,
};

pub struct EmitenListService {
    repo: EmitenListRepository,
}

impl EmitenListService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: EmitenListRepository::new(session),
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

        let proto_rows: Vec<EmitenListRow> = rows.into_iter().map(EmitenList::into_proto).collect();

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

        let row: Option<EmitenList> = self
            .repo
            .get_by_code_name(&code_name)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let row = row.ok_or_else(|| {
            Status::not_found(format!("emiten_list code_name={code_name} tidak ditemukan"))
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
}
