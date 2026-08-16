use std::pin::Pin;
use std::sync::Arc;

use chrono::{Datelike, Local};
use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, SessionStore};

use crate::model::HariLiburRow as DbHariLiburRow;
use crate::pb::hari_libur_server::HariLibur;
use crate::pb::{GetAllHariLiburRequest, GetAllHariLiburResponse, HariLiburRow};

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

        let token = extract_bearer_token(&request).map_err(|e| {
            eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
            e
        })?;
        let auth = validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|e| {
                eprintln!("{rpc_name} anonymous {}ms", started.elapsed().as_millis());
                Status::unauthenticated(e)
            })?;
        let user_name = auth.nama;

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
}
