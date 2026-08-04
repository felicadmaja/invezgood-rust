use std::pin::Pin;
use std::sync::Arc;

use chrono::{Local, NaiveDate, Timelike};
use futures::Stream;
use scylla::client::session::Session;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{BandarmologyEntryDb, BandarmologyRow as DbBandarmologyRow};
use crate::pb::bandarmology_server::Bandarmology;
use crate::pb::{
    BandarmologyEntry, BandarmologyRow, GetBandarmologyByCodeRequest,
    GetBandarmologyByCodeResponse,
};

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<GetBandarmologyByCodeResponse, Status>> + Send>>;

pub struct BandarmologyService {
    session: Arc<Session>,
    auth_sessions: SessionStore,
}

impl BandarmologyService {
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

    fn parse_trade_date(value: &str) -> Result<NaiveDate, Status> {
        NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!(
                "tahun_bulan_tanggal tidak valid (harus YYYY-MM-DD): {value}"
            ))
        })
    }

    fn ensure_today_data_available(trade_date: NaiveDate) -> Result<(), Status> {
        let now = Local::now();
        if trade_date == now.date_naive() && now.hour() < 18 {
            return Err(Status::failed_precondition(
                "Belum ada data bandarmology market berjalan",
            ));
        }
        Ok(())
    }

    fn entry_to_proto(row: BandarmologyEntryDb) -> BandarmologyEntry {
        BandarmologyEntry {
            code: row.code,
            buy_freq: row.buy_freq,
            buy_volume: row.buy_volume,
            buy_value: row.buy_value,
            sell_freq: row.sell_freq,
            sell_volume: row.sell_volume,
            sell_value: row.sell_value,
            buy_avg: row.buy_avg,
            sell_avg: row.sell_avg,
            net_value: row.net_value,
            net_volume: row.net_volume,
            net_freq: row.net_freq,
            name: row.name,
        }
    }

    fn row_to_proto(row: DbBandarmologyRow) -> BandarmologyRow {
        BandarmologyRow {
            code: row.code,
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            bandarmology: row
                .bandarmology
                .unwrap_or_default()
                .into_iter()
                .map(Self::entry_to_proto)
                .collect(),
            updated_at: row
                .updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
        }
    }

    async fn load_or_fetch(
        session: Arc<Session>,
        code: &str,
        trade_date: NaiveDate,
    ) -> Result<(DbBandarmologyRow, bool), Status> {
        if let Some(row) =
            crate::repository::find_by_code_and_date(session.as_ref(), code, trade_date)
                .await
                .map_err(Status::internal)?
        {
            if crate::repository::has_bandarmology_data(&row) {
                return Ok((row, false));
            }
        }

        let row = crate::invezgo::fetch_and_save(session, code, trade_date)
            .await
            .map_err(Status::internal)?;
        Ok((row, true))
    }
}

#[tonic::async_trait]
impl Bandarmology for BandarmologyService {
    type GetBandarmologyByCodeStream = ResponseStream;

    async fn get_bandarmology_by_code(
        &self,
        request: Request<GetBandarmologyByCodeRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let inner = request.into_inner();
        let code = inner.code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        if inner.tahun_bulan_tanggal.is_empty() {
            return Err(Status::invalid_argument(
                "tahun_bulan_tanggal wajib diisi (minimal 1 tanggal YYYY-MM-DD)",
            ));
        }

        let mut trade_dates = Vec::with_capacity(inner.tahun_bulan_tanggal.len());
        for date_str in inner.tahun_bulan_tanggal {
            let trade_date = Self::parse_trade_date(&date_str)?;
            Self::ensure_today_data_available(trade_date)?;
            trade_dates.push(trade_date);
        }

        let session = Arc::clone(&self.session);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let user_name_spawn = user_name.clone();

        tokio::spawn(async move {
            for trade_date in trade_dates {
                let item_started = std::time::Instant::now();
                let date_str = trade_date.format("%Y-%m-%d").to_string();
                match Self::load_or_fetch(Arc::clone(&session), &code, trade_date).await {
                    Ok((row, from_api)) => {
                        let elapsed = item_started.elapsed().as_millis();
                        if from_api {
                            eprintln!(
                                "\x1b[32mGetBandarmologyByCode {user_name_spawn} {elapsed}ms - GET summary/stock/{code}?from={date_str}&to={date_str}&investor=all&market=RG\x1b[0m"
                            );
                        } else {
                            eprintln!(
                                "GetBandarmologyByCode {user_name_spawn} {elapsed}ms - cache HIT {code} {date_str}"
                            );
                        }
                        if tx
                            .send(Ok(GetBandarmologyByCodeResponse {
                                item: Some(Self::row_to_proto(row)),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(status) => {
                        eprintln!(
                            "GetBandarmologyByCode {user_name_spawn} {}ms - error {code} {date_str}: {}",
                            item_started.elapsed().as_millis(),
                            status.message()
                        );
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }
}
