use std::sync::Arc;

use chrono::{Local, NaiveDate, Timelike};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{BandarmologyEntryDb, BandarmologyRow as DbBandarmologyRow};
use crate::pb::bandarmology_server::Bandarmology;
use crate::pb::{
    BandarmologyEntry, BandarmologyRow, GetBandarmologyByCodeRequest,
    GetBandarmologyByCodeResponse,
};

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

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
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
        trade_date: chrono::NaiveDate,
    ) -> Result<DbBandarmologyRow, Status> {
        if let Some(row) = crate::repository::find_by_code_and_date(session.as_ref(), code, trade_date)
            .await
            .map_err(Status::internal)?
        {
            if crate::repository::has_bandarmology_data(&row) {
                return Ok(row);
            }
        }

        crate::invezgo::fetch_and_save(session, code, trade_date)
            .await
            .map_err(Status::internal)
    }
}

#[tonic::async_trait]
impl Bandarmology for BandarmologyService {
    async fn get_bandarmology_by_code(
        &self,
        request: Request<GetBandarmologyByCodeRequest>,
    ) -> Result<Response<GetBandarmologyByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetBandarmologyByCodeResponse>, Status> = async {
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

            let mut items = Vec::with_capacity(inner.tahun_bulan_tanggal.len());
            for date_str in inner.tahun_bulan_tanggal {
                let trade_date = Self::parse_trade_date(&date_str)?;
                Self::ensure_today_data_available(trade_date)?;
                let row = Self::load_or_fetch(Arc::clone(&self.session), &code, trade_date).await?;
                items.push(Self::row_to_proto(row));
            }

            Ok(Response::new(GetBandarmologyByCodeResponse { items }))
        }
        .await;

        Self::log_rpc_debug("GetBandarmologyByCode", &user_name, started);
        result
    }
}
