use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::model::{Keystats, StockListRow as DbStockListRow};
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    GetKeyStatsRequest, GetStockListRequest, GetStockListResponse, KeyStatsColumn, KeyStatsRow,
    KeyStatsRowItem, KeyStatsValue, StockListRow,
};

const KEYSTATS_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

pub struct StockListService {
    session: Arc<Session>,
    redis: redis::Client,
}

impl StockListService {
    pub fn new(session: Arc<Session>) -> Result<Self, String> {
        let redis = crate::redis_cache::client_from_env()?;
        Ok(Self { session, redis })
    }

    fn should_refresh_keystats(updated_at: Option<DateTime<Utc>>) -> bool {
        let Some(updated_at) = updated_at else {
            return true;
        };
        Utc::now().timestamp() - updated_at.timestamp() > KEYSTATS_MAX_AGE_SECS
    }

    fn db_row_to_proto(row: DbStockListRow) -> StockListRow {
        StockListRow {
            code: row.code,
            name: row.name.unwrap_or_default(),
            sector: row.sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            keystats_updated_at: row.keystats_updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn keystats_to_proto(code: String, keystats: Keystats, updated_at: Option<DateTime<Utc>>) -> KeyStatsRow {
        KeyStatsRow {
            code,
            rows: keystats
                .rows
                .into_iter()
                .map(|row| KeyStatsRowItem {
                    id: row.id,
                    name: row.name,
                    values: row
                        .values
                        .into_iter()
                        .map(|v| KeyStatsValue {
                            col: v.col,
                            year: v.year,
                            amount: v.amount,
                            period: v.period,
                        })
                        .collect(),
                })
                .collect(),
            columns: keystats
                .columns
                .into_iter()
                .map(|c| KeyStatsColumn {
                    year: c.year,
                    label: c.label,
                    period: c.period,
                })
                .collect(),
            keystats_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn keystats_from_db_row(row: &DbStockListRow) -> Result<(Keystats, Option<DateTime<Utc>>), Status> {
        let Some(keystats_db) = row.keystats.clone() else {
            return Err(Status::not_found(format!(
                "keystats belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((Keystats::from(keystats_db), row.keystats_updated_at))
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_stock_list(
        &self,
        _request: Request<GetStockListRequest>,
    ) -> Result<Response<GetStockListResponse>, Status> {
        let mut redis_conn = self
            .redis
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| Status::internal(format!("redis connect: {e}")))?;

        let refresh = crate::redis_cache::should_refresh_from_api(&mut redis_conn)
            .await
            .map_err(Status::internal)?;

        let message = if refresh {
            let count = crate::invezgo::fetch_and_save(self.session.clone())
                .await
                .map_err(Status::internal)?;
            crate::redis_cache::set_updated_at(&mut redis_conn)
                .await
                .map_err(Status::internal)?;
            format!("refresh Invezgo: {count} saham disimpan ke stock_list")
        } else {
            "cache valid (<30 hari): baca dari Scylla".into()
        };

        let rows = crate::repository::token_ring_scan(self.session.as_ref())
            .await
            .map_err(Status::internal)?;

        let items = rows.into_iter().map(Self::db_row_to_proto).collect();

        Ok(Response::new(GetStockListResponse {
            success: true,
            message,
            items,
        }))
    }

    async fn get_key_stats(
        &self,
        request: Request<GetKeyStatsRequest>,
    ) -> Result<Response<KeyStatsRow>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh_keystats(row.keystats_updated_at))
            .unwrap_or(true);

        if refresh {
            let (keystats, updated_at) = crate::invezgo::fetch_and_save_keystats(
                self.session.clone(),
                &code,
            )
            .await
            .map_err(Status::internal)?;

            return Ok(Response::new(Self::keystats_to_proto(
                code,
                keystats,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (keystats, updated_at) = Self::keystats_from_db_row(&row)?;
        Ok(Response::new(Self::keystats_to_proto(
            code,
            keystats,
            updated_at,
        )))
    }
}
