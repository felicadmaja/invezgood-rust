use std::pin::Pin;
use std::sync::Arc;

use chrono::Local;
use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::hub::{SubscriberGuard, TodayPollHub};
use crate::model::TopGainerLoserRow as DbTopGainerLoserRow;
use crate::pb::top_gainer_loser_server::TopGainerLoser;
use crate::pb::{
    GetTopGainerLoserRequest, GetTopGainerLoserResponse, GraphPoint, TopGainerLoserRow,
};

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<GetTopGainerLoserResponse, Status>> + Send>>;

pub struct TopGainerLoserService {
    session: Arc<Session>,
    today_hub: Arc<TodayPollHub>,
    auth_sessions: SessionStore,
}

impl TopGainerLoserService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Self {
        let today_hub = Arc::new(TodayPollHub::new(session.clone()));
        Self {
            session,
            today_hub,
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

    fn rows_to_response(rows: Vec<DbTopGainerLoserRow>) -> GetTopGainerLoserResponse {
        GetTopGainerLoserResponse {
            items: rows.into_iter().map(Self::db_row_to_proto).collect(),
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

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        let result = if trade_date != today {
            let mut rows = crate::repository::find_by_date(self.session.as_ref(), trade_date)
                .await
                .map_err(Status::internal)?;
            if rows.is_empty() {
                rows = crate::invezgo::fetch_and_save(self.session.clone(), trade_date)
                    .await
                    .map_err(Status::internal)?;
            }
            let _ = tx
                .send(Ok(Self::rows_to_response(rows)))
                .await;
            drop(tx);
            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as ResponseStream))
        } else {
            let hub = Arc::clone(&self.today_hub);
            tokio::spawn(async move {
                let _guard = SubscriberGuard::new(Arc::clone(&hub));
                let mut broadcast_rx = hub.subscribe();

                hub.add_subscriber().await;

                if let Some(snapshot) = hub.last_snapshot().await {
                    if tx.send(Ok(Self::rows_to_response(snapshot))).await.is_err() {
                        return;
                    }
                }

                loop {
                    match broadcast_rx.recv().await {
                        Ok(rows) => {
                            if tx.send(Ok(Self::rows_to_response(rows))).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as ResponseStream))
        };

        Self::log_rpc_debug("GetTopGainerLoser", &user_name, started);
        result
    }
}
