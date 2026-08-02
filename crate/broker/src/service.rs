use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::BrokerRow as DbBrokerRow;
use crate::pb::broker_server::Broker;
use crate::pb::{
    BrokerRow, GetAllBrokersRequest, GetAllBrokersResponse, GetBrokerByCodeRequest,
    GetBrokerByCodeResponse,
};

pub struct BrokerService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl BrokerService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
            auth_sessions,
        }
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }

    fn row_to_proto(row: DbBrokerRow) -> BrokerRow {
        BrokerRow {
            broker_code: row.broker_code,
            name: row.name.unwrap_or_default(),
            tipe: row.tipe.unwrap_or_default(),
            asosiasi: row.asosiasi.unwrap_or_default(),
            catatan: row.catatan.unwrap_or_default(),
            updated_at: row
                .updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
        }
    }

    async fn load_all(session: Arc<Session>) -> Result<Vec<DbBrokerRow>, Status> {
        let mut rows = crate::repository::find_all(session.as_ref())
            .await
            .map_err(Status::internal)?;

        if rows.is_empty() {
            rows = crate::invezgo::fetch_and_save(session)
                .await
                .map_err(Status::internal)?;
        }

        Ok(rows)
    }
}

#[tonic::async_trait]
impl Broker for BrokerService {
    async fn get_all_brokers(
        &self,
        request: Request<GetAllBrokersRequest>,
    ) -> Result<Response<GetAllBrokersResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllBrokersResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = Self::load_all(Arc::clone(&self.session)).await?;

            Ok(Response::new(GetAllBrokersResponse {
                items: rows.into_iter().map(Self::row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllBrokers", &user_name, started);
        result
    }

    async fn get_broker_by_code(
        &self,
        request: Request<GetBrokerByCodeRequest>,
    ) -> Result<Response<GetBrokerByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetBrokerByCodeResponse>, Status> = async {
            let broker_code = request.into_inner().broker_code.trim().to_ascii_uppercase();
            if broker_code.is_empty() {
                return Err(Status::invalid_argument("broker_code wajib diisi"));
            }

            if let Some(row) = crate::repository::find_by_code(self.session.as_ref(), &broker_code)
                .await
                .map_err(Status::internal)?
            {
                return Ok(Response::new(GetBrokerByCodeResponse {
                    item: Some(Self::row_to_proto(row)),
                }));
            }

            Self::load_all(Arc::clone(&self.session)).await?;

            let row = crate::repository::find_by_code(self.session.as_ref(), &broker_code)
                .await
                .map_err(Status::internal)?;

            let Some(row) = row else {
                return Err(Status::not_found(format!(
                    "broker_code={broker_code} tidak ditemukan"
                )));
            };

            Ok(Response::new(GetBrokerByCodeResponse {
                item: Some(Self::row_to_proto(row)),
            }))
        }
        .await;

        Self::log_rpc_debug("GetBrokerByCode", &user_name, started);
        result
    }
}
