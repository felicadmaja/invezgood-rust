use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use grpc_stream::send_or_break;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::TopForeignFlowRow as DbTopForeignFlowRow;
use crate::pb::top_foreign_flow_server::TopForeignFlow;
use crate::pb::{
    GetTopForeignFlowByCodeRequest, GetTopForeignFlowByCodeResponse,
    GetTopForeignFlowByTanggalRequest, GetTopForeignFlowByTanggalResponse, TopForeignFlowRow,
};

type TanggalStream =
    Pin<Box<dyn Stream<Item = Result<GetTopForeignFlowByTanggalResponse, Status>> + Send>>;

pub struct TopForeignFlowService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl TopForeignFlowService {
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

    fn db_row_to_proto(row: DbTopForeignFlowRow) -> TopForeignFlowRow {
        TopForeignFlowRow {
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            code: row.code,
            name: row.name.unwrap_or_default(),
            price: row.price.unwrap_or_default(),
            change: row.change.unwrap_or_default(),
            value: row.value,
            volume: row.volume.unwrap_or_default(),
            accum_or_dist: row.accum_or_dist.unwrap_or_default(),
        }
    }

    fn map_sync_error(e: String) -> Status {
        if e.contains("Sabtu") || e.contains("hari ini") {
            Status::failed_precondition(e)
        } else {
            Status::internal(e)
        }
    }
}

#[tonic::async_trait]
impl TopForeignFlow for TopForeignFlowService {
    type GetTopForeignFlowByTanggalStream = TanggalStream;

    async fn get_top_foreign_flow_by_tanggal(
        &self,
        request: Request<GetTopForeignFlowByTanggalRequest>,
    ) -> Result<Response<TanggalStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let inner = request.into_inner();

        if inner.tahun_bulan_tanggal.is_empty() {
            Self::log_rpc_debug("GetTopForeignFlowByTanggal", &user_name, started);
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal wajib diisi (≥1 tanggal YYYY-MM-DD)",
            ));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let session = self.session.clone();
        let dates = inner.tahun_bulan_tanggal;
        let user_name_bg = user_name.clone();

        tokio::spawn(async move {
            let mut aborted = false;

            for raw_date in dates {
                let trade_date = match crate::invezgo::parse_trade_date(&raw_date) {
                    Ok(d) => d,
                    Err(e) => {
                        let _ = send_or_break(
                            &tx,
                            Err(Status::invalid_argument(format!("{raw_date}: {e}"))),
                        )
                        .await;
                        aborted = true;
                        break;
                    }
                };

                let outcome = match crate::sync::sync_trade_date(session.clone(), trade_date).await
                {
                    Ok(o) => o,
                    Err(e) => {
                        let _ = send_or_break(&tx, Err(Self::map_sync_error(e))).await;
                        aborted = true;
                        break;
                    }
                };

                if outcome.cached {
                    eprintln!(
                        "GetTopForeignFlowByTanggal {user_name_bg} skip Invezgo API date={trade_date} (MV ada ≥1 baris)"
                    );
                }

                let n = outcome.rows.len();
                let message = if outcome.cached {
                    format!("{trade_date}: {n} baris dari cache Scylla")
                } else {
                    format!(
                        "{trade_date}: fetch Invezgo {} baris upsert, {n} baris",
                        outcome.saved
                    )
                };

                let response = GetTopForeignFlowByTanggalResponse {
                    success: true,
                    message,
                    items: outcome
                        .rows
                        .into_iter()
                        .map(Self::db_row_to_proto)
                        .collect(),
                };

                if !send_or_break(&tx, Ok(response)).await {
                    eprintln!(
                        "GetTopForeignFlowByTanggal {user_name_bg} client disconnect date={trade_date}"
                    );
                    aborted = true;
                    break;
                }
            }

            if !aborted {
                eprintln!("GetTopForeignFlowByTanggal {user_name_bg} stream selesai");
            }
            drop(tx);
            Self::log_rpc_debug("GetTopForeignFlowByTanggal", &user_name_bg, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as TanggalStream
        ))
    }

    async fn get_top_foreign_flow_by_code(
        &self,
        request: Request<GetTopForeignFlowByCodeRequest>,
    ) -> Result<Response<GetTopForeignFlowByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetTopForeignFlowByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let rows = crate::repository::find_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(GetTopForeignFlowByCodeResponse {
                success: true,
                message: format!("{} baris top foreign flow code={code}", rows.len()),
                items: rows.into_iter().map(Self::db_row_to_proto).collect(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetTopForeignFlowByCode", &user_name, started);
        result
    }
}
