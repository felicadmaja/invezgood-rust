use std::pin::Pin;
use std::sync::Arc;

use chrono::{Datelike, Local, NaiveDate, Utc};
use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, SessionStore};

use crate::model::HariLiburRow as DbHariLiburRow;
use crate::pb::hari_libur_server::HariLibur;
use crate::pb::{
    DeleteHariLiburRequest, DeleteHariLiburResponse, GetAllHariLiburRequest,
    GetAllHariLiburResponse, HariLiburRow, InsertHariLiburRequest, InsertHariLiburResponse,
    UpdateHariLiburRequest, UpdateHariLiburResponse,
};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<GetAllHariLiburResponse, Status>> + Send>>;

pub struct HariLiburService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl HariLiburService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        Self {
            session,
            auth_sessions,
        }
    }

    /// Nama user dari metadata `authorization: Bearer <token>`.
    async fn require_auth<T>(&self, request: &Request<T>) -> Result<String, Status> {
        let token = extract_bearer_token(request)?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(Status::unauthenticated)?;
        Ok(auth.nama)
    }

    fn parse_date(value: &str) -> Result<NaiveDate, Status> {
        NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!("date tidak valid (harus YYYY-MM-DD): {value}"))
        })
    }

    fn row_to_proto(row: DbHariLiburRow) -> HariLiburRow {
        HariLiburRow {
            date: row.date.format("%Y-%m-%d").to_string(),
            tahun: row.tahun.unwrap_or_default(),
            name: row.name.unwrap_or_default(),
            is_civic: row.is_civic.unwrap_or(false),
            is_religious: row.is_religious.unwrap_or(false),
            is_cuti_bersama: row.is_cuti_bersama.unwrap_or(false),
            updated_at: row
                .updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl HariLibur for HariLiburService {
    type GetAllHariLiburStream = ResponseStream;

    async fn get_all_hari_libur(
        &self,
        request: Request<GetAllHariLiburRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "GetAllHariLibur";

        let user_name = match self.require_auth(&request).await {
            Ok(nama) => nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let tahun = match request.into_inner().tahun {
            Some(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => Local::now().year().to_string(),
        };
        if tahun.len() != 4 || !tahun.chars().all(|c| c.is_ascii_digit()) {
            eprintln!(
                "{rpc_name} {user_name} {tahun} {}ms",
                started.elapsed().as_millis()
            );
            return Err(Status::invalid_argument(format!(
                "tahun tidak valid (harus YYYY): {tahun}"
            )));
        }

        let session = Arc::clone(&self.session);
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            let result = crate::repository::find_by_tahun(session.as_ref(), &tahun).await;
            let rows = match result {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!(
                        "{rpc_name} {user_name} {tahun} {}ms - error: {e}",
                        started.elapsed().as_millis()
                    );
                    let _ = tx.send(Err(Status::internal(e))).await;
                    return;
                }
            };

            let total = rows.len();
            for row in rows {
                if tx
                    .send(Ok(GetAllHariLiburResponse {
                        item: Some(Self::row_to_proto(row)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            eprintln!(
                "{rpc_name} {user_name} {tahun} {}ms - {total} tanggal",
                started.elapsed().as_millis()
            );
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }

    async fn insert_hari_libur(
        &self,
        request: Request<InsertHariLiburRequest>,
    ) -> Result<Response<InsertHariLiburResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "InsertHariLibur";

        let user_name = match self.require_auth(&request).await {
            Ok(nama) => nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let inner = request.into_inner();
        let date_log = inner.date.trim().to_string();

        let result: Result<Response<InsertHariLiburResponse>, Status> = async {
            let date = Self::parse_date(&inner.date)?;
            let name = inner.name.trim().to_string();
            if name.is_empty() {
                return Err(Status::invalid_argument("name wajib diisi"));
            }

            let row = DbHariLiburRow {
                date,
                tahun: Some(date.year().to_string()),
                name: Some(name),
                is_civic: Some(inner.is_civic),
                is_religious: Some(inner.is_religious),
                is_cuti_bersama: Some(inner.is_cuti_bersama),
                updated_at: Some(Utc::now()),
            };

            crate::repository::upsert(self.session.as_ref(), &row)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(InsertHariLiburResponse {
                success: true,
                message: format!("hari libur {date} berhasil disimpan"),
            }))
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {date_log} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn update_hari_libur(
        &self,
        request: Request<UpdateHariLiburRequest>,
    ) -> Result<Response<UpdateHariLiburResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "UpdateHariLibur";

        let user_name = match self.require_auth(&request).await {
            Ok(nama) => nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let inner = request.into_inner();
        let date_log = inner.date.trim().to_string();

        let result: Result<Response<UpdateHariLiburResponse>, Status> = async {
            let date = Self::parse_date(&inner.date)?;
            let name = inner.name.trim().to_string();
            if name.is_empty() {
                return Err(Status::invalid_argument("name wajib diisi"));
            }

            let row = DbHariLiburRow {
                date,
                tahun: Some(date.year().to_string()),
                name: Some(name),
                is_civic: Some(inner.is_civic),
                is_religious: Some(inner.is_religious),
                is_cuti_bersama: Some(inner.is_cuti_bersama),
                updated_at: Some(Utc::now()),
            };

            let updated = crate::repository::update(self.session.as_ref(), &row)
                .await
                .map_err(Status::internal)?;
            if !updated {
                return Err(Status::not_found(format!(
                    "hari libur {date} tidak ditemukan"
                )));
            }

            Ok(Response::new(UpdateHariLiburResponse {
                success: true,
                message: format!("hari libur {date} berhasil diupdate"),
            }))
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {date_log} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn delete_hari_libur(
        &self,
        request: Request<DeleteHariLiburRequest>,
    ) -> Result<Response<DeleteHariLiburResponse>, Status> {
        let started = std::time::Instant::now();
        let rpc_name = "DeleteHariLibur";

        let user_name = match self.require_auth(&request).await {
            Ok(nama) => nama,
            Err(e) => {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                return Err(e);
            }
        };

        let inner = request.into_inner();
        let date_log = inner.date.trim().to_string();

        let result: Result<Response<DeleteHariLiburResponse>, Status> = async {
            let date = Self::parse_date(&inner.date)?;

            let deleted = crate::repository::delete(self.session.as_ref(), date)
                .await
                .map_err(Status::internal)?;
            if !deleted {
                return Err(Status::not_found(format!(
                    "hari libur {date} tidak ditemukan"
                )));
            }

            Ok(Response::new(DeleteHariLiburResponse {
                success: true,
                message: format!("hari libur {date} berhasil dihapus"),
            }))
        }
        .await;

        eprintln!(
            "{rpc_name} {user_name} {date_log} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
