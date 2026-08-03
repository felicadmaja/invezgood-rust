use std::pin::Pin;
use std::sync::Arc;

use chrono::Local;
use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::TopGainerLoserRow as DbTopGainerLoserRow;
use crate::pb::top_gainer_loser_server::TopGainerLoser;
use crate::pb::{
    GetTopGainerLoserRequest, GetTopGainerLoserResponse, GraphPoint, TopGainerLoserRow,
};

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<GetTopGainerLoserResponse, Status>> + Send>>;

pub struct TopGainerLoserService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl TopGainerLoserService {
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

    fn success_response(
        rows: Vec<DbTopGainerLoserRow>,
        empty_message: impl Into<String>,
    ) -> GetTopGainerLoserResponse {
        let n = rows.len();
        let message = if n == 0 {
            empty_message.into()
        } else {
            format!("{n} baris top gainer/loser")
        };
        GetTopGainerLoserResponse {
            success: true,
            message,
            items: rows.into_iter().map(Self::db_row_to_proto).collect(),
        }
    }

    fn error_response(message: String) -> GetTopGainerLoserResponse {
        GetTopGainerLoserResponse {
            success: false,
            message,
            items: vec![],
        }
    }

    fn db_row_to_proto(row: DbTopGainerLoserRow) -> TopGainerLoserRow {
        TopGainerLoserRow {
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            code: row.code,
            name: row.name.unwrap_or_default(),
            price: row.price.unwrap_or_default(),
            change: row.change_pct.unwrap_or_default(),
            value: row.value.unwrap_or_default(),
            volume: row.volume.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            calculated_value: row.calculated_value.unwrap_or_default(),
            tipe: row.tipe.unwrap_or_default(),
            graph: row
                .graph
                .unwrap_or_default()
                .into_iter()
                .map(|point| GraphPoint {
                    date: point.date,
                    value: point.value,
                })
                .collect(),
        }
    }
}

#[tonic::async_trait]
impl TopGainerLoser for TopGainerLoserService {
    type GetTopGainerLoserStream = ResponseStream;

    async fn get_top_gainer_loser(
        &self,
        request: Request<GetTopGainerLoserRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let trade_date =
            crate::invezgo::resolve_trade_date(request.into_inner().tahun_bulan_tanggal)
                .map_err(Status::invalid_argument)?;
        let today = Local::now().date_naive();
        let is_today = trade_date == today;

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        let rows = crate::repository::find_by_date(self.session.as_ref(), trade_date)
            .await
            .map_err(Status::internal)?;

        let response = if !rows.is_empty() {
            Self::success_response(rows, "")
        } else if is_today {
            // Hari ini: jangan GET Invezgo — API biasanya kosong dan membuang jatah.
            eprintln!(
                "GetTopGainerLoser {user_name} skip Invezgo API date={trade_date} (hari ini, Scylla kosong)"
            );
            Self::success_response(
                vec![],
                "hari ini: tidak ada data di Scylla; Invezgo API dilewati (hasil biasanya kosong)",
            )
        } else {
            match crate::invezgo::fetch_and_save(self.session.clone(), trade_date).await {
                Ok(fetched) => Self::success_response(
                    fetched,
                    "Invezgo mengembalikan data kosong (gain=0, loss=0)",
                ),
                Err(error) => Self::error_response(error),
            }
        };

        eprintln!(
            "GetTopGainerLoser {user_name} push date={trade_date} today={is_today} success={} items={} msg={}",
            response.success,
            response.items.len(),
            response.message
        );
        let _ = tx.send(Ok(response)).await;
        drop(tx);

        Self::log_rpc_debug("GetTopGainerLoser", &user_name, started);
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }
}
