use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::broker_server::Broker as BrokerRpc;
use crate::model::Broker;
use crate::repository::BrokerRepository;
use crate::{
    BrokerRow, DeleteBrokerRequest, DeleteBrokerResponse, GetAllBrokerRequest, GetAllBrokerResponse,
    GetBrokerByBrokerCodeRequest, GetBrokerByBrokerCodeResponse, InsertBrokerRequest,
    InsertBrokerResponse, UpdateBrokerRequest, UpdateBrokerResponse,
};

pub struct BrokerService {
    repo: BrokerRepository,
}

impl BrokerService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: BrokerRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl BrokerRpc for BrokerService {
    async fn get_all_broker(
        &self,
        request: Request<GetAllBrokerRequest>,
    ) -> Result<Response<GetAllBrokerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let rows = self
            .repo
            .get_all()
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        let proto_rows: Vec<BrokerRow> = rows.into_iter().map(Broker::into_proto).collect();

        println!(
            "GetAllBroker {} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetAllBrokerResponse {
            rows: proto_rows,
        }))
    }

    async fn get_broker_by_broker_code(
        &self,
        request: Request<GetBrokerByBrokerCodeRequest>,
    ) -> Result<Response<GetBrokerByBrokerCodeResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let broker_code = request.into_inner().broker_code.trim().to_ascii_uppercase();
        if broker_code.is_empty() {
            return Err(Status::invalid_argument("broker_code wajib diisi"));
        }

        let row = self
            .repo
            .get_by_broker_code(&broker_code)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("broker_code={broker_code} tidak ditemukan"))
            })?;

        println!(
            "GetBrokerByBrokerCode {} {broker_code} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetBrokerByBrokerCodeResponse {
            broker: Some(row.into_proto()),
        }))
    }

    async fn insert_broker(
        &self,
        request: Request<InsertBrokerRequest>,
    ) -> Result<Response<InsertBrokerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let Some(row) = req.broker else {
            return Ok(Response::new(InsertBrokerResponse {
                success: false,
                message: "broker wajib diisi".to_string(),
            }));
        };

        let broker = Broker::from_proto(row);
        if broker.broker_code.is_empty() {
            return Ok(Response::new(InsertBrokerResponse {
                success: false,
                message: "broker_code wajib diisi".to_string(),
            }));
        }

        let inserted = self
            .repo
            .insert(
                &broker.broker_code,
                &broker.name,
                &broker.tipe,
                &broker.asosiasi,
                &broker.catatan,
            )
            .await
            .map_err(|e| Status::internal(format!("Scylla insert failed: {e}")))?;

        let broker_code = broker.broker_code.clone();
        let (success, message) = if inserted {
            (
                true,
                format!("broker {broker_code} berhasil diinsert"),
            )
        } else {
            (
                false,
                format!("broker_code={broker_code} sudah ada"),
            )
        };

        println!(
            "InsertBroker {} {broker_code} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(InsertBrokerResponse {
            success,
            message,
        }))
    }

    async fn update_broker(
        &self,
        request: Request<UpdateBrokerRequest>,
    ) -> Result<Response<UpdateBrokerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let Some(row) = req.broker else {
            return Ok(Response::new(UpdateBrokerResponse {
                success: false,
                message: "broker wajib diisi".to_string(),
            }));
        };

        let broker = Broker::from_proto(row);
        if broker.broker_code.is_empty() {
            return Ok(Response::new(UpdateBrokerResponse {
                success: false,
                message: "broker_code wajib diisi".to_string(),
            }));
        }

        let updated = self
            .repo
            .update(
                &broker.broker_code,
                &broker.name,
                &broker.tipe,
                &broker.asosiasi,
                &broker.catatan,
            )
            .await
            .map_err(|e| Status::internal(format!("Scylla update failed: {e}")))?;

        let broker_code = broker.broker_code.clone();
        let (success, message) = if updated {
            (
                true,
                format!("broker {broker_code} berhasil diupdate"),
            )
        } else {
            (
                false,
                format!("broker_code={broker_code} tidak ditemukan"),
            )
        };

        println!(
            "UpdateBroker {} {broker_code} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(UpdateBrokerResponse {
            success,
            message,
        }))
    }

    async fn delete_broker(
        &self,
        request: Request<DeleteBrokerRequest>,
    ) -> Result<Response<DeleteBrokerResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();

        let broker_code = request.into_inner().broker_code.trim().to_ascii_uppercase();
        if broker_code.is_empty() {
            return Ok(Response::new(DeleteBrokerResponse {
                success: false,
                message: "broker_code wajib diisi".to_string(),
            }));
        }

        let deleted = self
            .repo
            .delete(&broker_code)
            .await
            .map_err(|e| Status::internal(format!("Scylla delete failed: {e}")))?;

        let (success, message) = if deleted {
            (true, format!("broker {broker_code} berhasil dihapus"))
        } else {
            (
                false,
                format!("broker_code={broker_code} tidak ditemukan"),
            )
        };

        println!(
            "DeleteBroker {} {broker_code} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(DeleteBrokerResponse {
            success,
            message,
        }))
    }
}
