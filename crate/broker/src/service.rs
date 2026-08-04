use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{BrokerRow as DbBrokerRow, BrokerStalkerRow as DbBrokerStalkerRow};
use crate::pb::broker_server::Broker;
use crate::pb::{
    BrokerRow, BrokerStalkerFromInvezgoRequest, BrokerStalkerFromInvezgoResponse,
    BrokerStalkerListItem, BrokerStalkerRow, DeleteBrokerByCodeRequest,
    DeleteBrokerByCodeResponse, GetAllBrokersRequest, GetAllBrokersResponse,
    GetBrokerByCodeRequest, GetBrokerByCodeResponse, TipeBroker, UpdateBrokerByCodeRequest,
    UpdateBrokerByCodeResponse,
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

    fn tipe_to_proto(value: Option<i8>) -> Option<i32> {
        Some(i32::from(value.unwrap_or(0)))
    }

    fn tipe_from_proto(value: i32) -> Result<i8, Status> {
        TipeBroker::try_from(value)
            .map(|tipe| tipe as i8)
            .map_err(|_| Status::invalid_argument(format!("tipe tidak valid: {value}")))
    }

    fn row_to_proto(row: DbBrokerRow) -> BrokerRow {
        BrokerRow {
            broker_code: row.broker_code,
            name: row.name.unwrap_or_default(),
            tipe: Self::tipe_to_proto(row.tipe),
            asosiasi: row.asosiasi.unwrap_or_default(),
            catatan: row.catatan.unwrap_or_default(),
            updated_at: row
                .updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
            is_huge: row.is_huge.unwrap_or(false),
        }
    }

    fn stalker_row_to_proto(row: DbBrokerStalkerRow) -> BrokerStalkerRow {
        BrokerStalkerRow {
            broker_code: row.broker_code,
            tahun_bulan: row.tahun_bulan,
            summary: row.summary.unwrap_or_default(),
            list: row
                .list
                .unwrap_or_default()
                .into_iter()
                .map(|fields| BrokerStalkerListItem { fields })
                .collect(),
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

    async fn update_broker_by_code(
        &self,
        request: Request<UpdateBrokerByCodeRequest>,
    ) -> Result<Response<UpdateBrokerByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateBrokerByCodeResponse>, Status> = async {
            let inner = request.into_inner();
            let broker_code = inner.broker_code.trim().to_ascii_uppercase();
            if broker_code.is_empty() {
                return Err(Status::invalid_argument("broker_code wajib diisi"));
            }

            if inner.name.is_none()
                && inner.tipe.is_none()
                && inner.asosiasi.is_none()
                && inner.catatan.is_none()
                && inner.is_huge.is_none()
            {
                return Err(Status::invalid_argument(
                    "minimal satu field update wajib diisi (name, tipe, asosiasi, catatan, is_huge)",
                ));
            }

            let Some(mut row) =
                crate::repository::find_by_code(self.session.as_ref(), &broker_code)
                    .await
                    .map_err(Status::internal)?
            else {
                return Err(Status::not_found(format!(
                    "broker_code={broker_code} tidak ditemukan"
                )));
            };

            if let Some(name) = inner.name {
                row.name = Some(name);
            }
            if let Some(tipe) = inner.tipe {
                row.tipe = Some(Self::tipe_from_proto(tipe)?);
            }
            if let Some(asosiasi) = inner.asosiasi {
                row.asosiasi = Some(asosiasi);
            }
            if let Some(catatan) = inner.catatan {
                row.catatan = Some(catatan);
            }
            if let Some(is_huge) = inner.is_huge {
                row.is_huge = Some(is_huge);
            }
            row.updated_at = Some(Utc::now());

            crate::repository::update_by_code(self.session.as_ref(), &row)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(UpdateBrokerByCodeResponse {
                item: Some(Self::row_to_proto(row)),
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateBrokerByCode", &user_name, started);
        result
    }

    async fn delete_broker_by_code(
        &self,
        request: Request<DeleteBrokerByCodeRequest>,
    ) -> Result<Response<DeleteBrokerByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<DeleteBrokerByCodeResponse>, Status> = async {
            let broker_code = request.into_inner().broker_code.trim().to_ascii_uppercase();
            if broker_code.is_empty() {
                return Err(Status::invalid_argument("broker_code wajib diisi"));
            }

            let exists = crate::repository::find_by_code(self.session.as_ref(), &broker_code)
                .await
                .map_err(Status::internal)?
                .is_some();

            if !exists {
                return Err(Status::not_found(format!(
                    "broker_code={broker_code} tidak ditemukan"
                )));
            }

            crate::repository::delete_by_code(self.session.as_ref(), &broker_code)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(DeleteBrokerByCodeResponse {
                success: true,
                message: format!("broker_code={broker_code} berhasil dihapus"),
            }))
        }
        .await;

        Self::log_rpc_debug("DeleteBrokerByCode", &user_name, started);
        result
    }

    async fn broker_stalker_from_invezgo(
        &self,
        request: Request<BrokerStalkerFromInvezgoRequest>,
    ) -> Result<Response<BrokerStalkerFromInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let mut from_api = false;
        let mut log_broker = String::new();
        let mut log_bulan = String::new();

        let result: Result<Response<BrokerStalkerFromInvezgoResponse>, Status> = async {
            let inner = request.into_inner();
            let broker_code = inner.broker_code.trim().to_ascii_uppercase();
            if broker_code.is_empty() {
                return Err(Status::invalid_argument("broker_code wajib diisi"));
            }

            let tahun_bulan = inner.tahun_bulan.trim().to_string();
            if tahun_bulan.is_empty() {
                return Err(Status::invalid_argument(
                    "tahun_bulan wajib diisi (YYYY-MM)",
                ));
            }
            crate::invezgo::month_range(&tahun_bulan).map_err(Status::invalid_argument)?;

            log_broker = broker_code.clone();
            log_bulan = tahun_bulan.clone();

            if let Some(row) =
                crate::repository::find_stalker(self.session.as_ref(), &broker_code, &tahun_bulan)
                    .await
                    .map_err(Status::internal)?
            {
                return Ok(Response::new(BrokerStalkerFromInvezgoResponse {
                    item: Some(Self::stalker_row_to_proto(row)),
                }));
            }

            from_api = true;
            let row = crate::invezgo::fetch_stalker_and_save(
                Arc::clone(&self.session),
                &broker_code,
                &tahun_bulan,
            )
            .await
            .map_err(Status::internal)?;

            Ok(Response::new(BrokerStalkerFromInvezgoResponse {
                item: Some(Self::stalker_row_to_proto(row)),
            }))
        }
        .await;

        let elapsed = started.elapsed().as_millis();
        if !log_broker.is_empty() {
            if from_api {
                eprintln!(
                    "\x1b[32mBrokerStalkerFromInvezgo {user_name} {elapsed}ms - {log_broker} {log_bulan}\x1b[0m"
                );
            } else {
                eprintln!(
                    "\x1b[37mBrokerStalkerFromInvezgo {user_name} {elapsed}ms - {log_broker} {log_bulan}\x1b[0m"
                );
            }
        } else {
            Self::log_rpc_debug("BrokerStalkerFromInvezgo", &user_name, started);
        }

        result
    }
}
