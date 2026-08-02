use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{
    BandarmologyBrokerBuyDb, BandarmologyBrokerSellDb, BandarmologyDayDb,
    BandarmologyHarianRow as DbBandarmologyHarianRow, BandarmologyRow as DbBandarmologyRow,
    BandarmologyTopStatsDb, PortofolioBandarmologyRow as DbPortofolioBandarmologyRow,
};
use crate::pb::bandarmology_server::Bandarmology;
use crate::pb::{
    BandarmologyBrokerBuy, BandarmologyBrokerSell, BandarmologyDay, BandarmologyHarianRow,
    BandarmologyRow, BandarmologyTopStats, GetBandarmologyByCodeAndMonthRequest,
    GetBandarmologyByCodeAndMonthResponse, GetBandarmologyHarianByCodeRequest,
    GetBandarmologyHarianByCodeResponse, GetPortofolioBandarmologyByCodeRequest,
    GetPortofolioBandarmologyByCodeResponse, PortofolioBandarmologyRow,
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

    fn parse_trade_date(value: &str) -> Result<chrono::NaiveDate, Status> {
        chrono::NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").map_err(|_| {
            Status::invalid_argument(format!(
                "tahun_bulan_tanggal tidak valid (harus YYYY-MM-DD): {value}"
            ))
        })
    }

    fn top_stats_to_proto(row: BandarmologyTopStatsDb) -> BandarmologyTopStats {
        BandarmologyTopStats {
            volume: row.volume,
            percent: row.percent,
            rp_b: row.rp_b,
            acc_dist: row.acc_dist,
        }
    }

    fn broker_buy_to_proto(row: BandarmologyBrokerBuyDb) -> BandarmologyBrokerBuy {
        BandarmologyBrokerBuy {
            broker_code: row.broker_code,
            buy_volume: row.buy_volume,
            buy_lot: row.buy_lot,
            buy_avg: row.buy_avg,
        }
    }

    fn broker_sell_to_proto(row: BandarmologyBrokerSellDb) -> BandarmologyBrokerSell {
        BandarmologyBrokerSell {
            broker_code: row.broker_code,
            sell_volume: row.sell_volume,
            sell_lot: row.sell_lot,
            sell_avg: row.sell_avg,
        }
    }

    fn day_to_proto(row: BandarmologyDayDb) -> BandarmologyDay {
        BandarmologyDay {
            top_1: Some(Self::top_stats_to_proto(row.top_1)),
            top_3: Some(Self::top_stats_to_proto(row.top_3)),
            top_5: Some(Self::top_stats_to_proto(row.top_5)),
            average: Some(Self::top_stats_to_proto(row.average)),
            net_volume: row.net_volume,
            net_value: row.net_value,
            average_rp: row.average_rp,
            broker_buy: row
                .broker_buy
                .unwrap_or_default()
                .into_iter()
                .map(Self::broker_buy_to_proto)
                .collect(),
            broker_sell: row
                .broker_sell
                .unwrap_or_default()
                .into_iter()
                .map(Self::broker_sell_to_proto)
                .collect(),
        }
    }

    fn optional_day_to_proto(day: Option<BandarmologyDayDb>) -> Option<BandarmologyDay> {
        day.map(Self::day_to_proto)
    }

    fn format_updated_at(value: Option<chrono::DateTime<chrono::Utc>>) -> String {
        value
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_default()
    }

    fn bandarmology_row_to_proto(row: DbBandarmologyRow) -> BandarmologyRow {
        BandarmologyRow {
            agg_tahun_bulan_emiten_name: row.agg_tahun_bulan_emiten_name,
            emiten_name: row.emiten_name.unwrap_or_default(),
            tahun_bulan: row.tahun_bulan.unwrap_or_default(),
            broker_summary: Self::optional_day_to_proto(row.broker_summary),
            broker_summary_current_w1: Self::optional_day_to_proto(row.broker_summary_current_w1),
            broker_summary_current_w2: Self::optional_day_to_proto(row.broker_summary_current_w2),
            broker_summary_current_w3: Self::optional_day_to_proto(row.broker_summary_current_w3),
            broker_summary_current_w4: Self::optional_day_to_proto(row.broker_summary_current_w4),
            updated_at: Self::format_updated_at(row.updated_at),
        }
    }

    fn harian_row_to_proto(row: DbBandarmologyHarianRow) -> BandarmologyHarianRow {
        BandarmologyHarianRow {
            emiten_name: row.emiten_name,
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            broker_summary_harian: Self::optional_day_to_proto(row.broker_summary_harian),
            updated_at: Self::format_updated_at(row.updated_at),
        }
    }

    fn portofolio_row_to_proto(row: DbPortofolioBandarmologyRow) -> PortofolioBandarmologyRow {
        PortofolioBandarmologyRow {
            emiten_name: row.emiten_name,
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            bandarmology: Self::optional_day_to_proto(row.bandarmology),
        }
    }
}

#[tonic::async_trait]
impl Bandarmology for BandarmologyService {
    async fn get_bandarmology_by_code_and_month(
        &self,
        request: Request<GetBandarmologyByCodeAndMonthRequest>,
    ) -> Result<Response<GetBandarmologyByCodeAndMonthResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetBandarmologyByCodeAndMonthResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            let tahun_bulan = inner.tahun_bulan.trim().to_string();

            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }
            if tahun_bulan.is_empty() {
                return Err(Status::invalid_argument("tahun_bulan wajib diisi (YYYY-MM)"));
            }
            if chrono::NaiveDate::parse_from_str(&format!("{tahun_bulan}-01"), "%Y-%m-%d").is_err()
            {
                return Err(Status::invalid_argument(
                    "tahun_bulan tidak valid (harus YYYY-MM)",
                ));
            }

            let row = crate::repository::find_by_code_and_month(
                self.session.as_ref(),
                &code,
                &tahun_bulan,
            )
            .await
            .map_err(Status::internal)?;

            let Some(row) = row else {
                return Err(Status::not_found(format!(
                    "bandarmology code={code} tahun_bulan={tahun_bulan} tidak ditemukan"
                )));
            };

            Ok(Response::new(GetBandarmologyByCodeAndMonthResponse {
                item: Some(Self::bandarmology_row_to_proto(row)),
            }))
        }
        .await;

        Self::log_rpc_debug("GetBandarmologyByCodeAndMonth", &user_name, started);
        result
    }

    async fn get_bandarmology_harian_by_code(
        &self,
        request: Request<GetBandarmologyHarianByCodeRequest>,
    ) -> Result<Response<GetBandarmologyHarianByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetBandarmologyHarianByCodeResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let trade_date = match inner.tahun_bulan_tanggal.as_deref() {
                Some(value) if !value.trim().is_empty() => {
                    Some(Self::parse_trade_date(value)?)
                }
                _ => None,
            };

            let row =
                crate::repository::find_harian_by_code(self.session.as_ref(), &code, trade_date)
                    .await
                    .map_err(Status::internal)?;

            let Some(row) = row else {
                let detail = trade_date
                    .map(|d| format!(" date={d}"))
                    .unwrap_or_else(|| " (terbaru)".to_string());
                return Err(Status::not_found(format!(
                    "bandarmology_harian code={code}{detail} tidak ditemukan"
                )));
            };

            Ok(Response::new(GetBandarmologyHarianByCodeResponse {
                item: Some(Self::harian_row_to_proto(row)),
            }))
        }
        .await;

        Self::log_rpc_debug("GetBandarmologyHarianByCode", &user_name, started);
        result
    }

    async fn get_portofolio_bandarmology_by_code(
        &self,
        request: Request<GetPortofolioBandarmologyByCodeRequest>,
    ) -> Result<Response<GetPortofolioBandarmologyByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetPortofolioBandarmologyByCodeResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let trade_date = match inner.tahun_bulan_tanggal.as_deref() {
                Some(value) if !value.trim().is_empty() => {
                    Some(Self::parse_trade_date(value)?)
                }
                _ => None,
            };

            let row = crate::repository::find_portofolio_by_code(
                self.session.as_ref(),
                &code,
                trade_date,
            )
            .await
            .map_err(Status::internal)?;

            let Some(row) = row else {
                let detail = trade_date
                    .map(|d| format!(" date={d}"))
                    .unwrap_or_else(|| " (terbaru)".to_string());
                return Err(Status::not_found(format!(
                    "portofolio_bandarmology code={code}{detail} tidak ditemukan"
                )));
            };

            Ok(Response::new(GetPortofolioBandarmologyByCodeResponse {
                item: Some(Self::portofolio_row_to_proto(row)),
            }))
        }
        .await;

        Self::log_rpc_debug("GetPortofolioBandarmologyByCode", &user_name, started);
        result
    }
}
