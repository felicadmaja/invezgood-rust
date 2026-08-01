use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::model::StockListRow as DbStockListRow;
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    GetStockListFromInvezgoRequest, GetStockListFromInvezgoResponse, GetStockListFromScyllaRequest,
    GetStockListFromScyllaResponse, StockListRow,
};

pub struct StockListService {
    session: Arc<Session>,
}

impl StockListService {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn db_row_to_proto(row: DbStockListRow) -> StockListRow {
        StockListRow {
            code: row.code,
            name: row.name.unwrap_or_default(),
            sector: row.sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_stock_list_from_invezgo(
        &self,
        _request: Request<GetStockListFromInvezgoRequest>,
    ) -> Result<Response<GetStockListFromInvezgoResponse>, Status> {
        match crate::invezgo::fetch_and_save(self.session.clone()).await {
            Ok(count) => Ok(Response::new(GetStockListFromInvezgoResponse {
                success: true,
                message: format!("{count} saham disimpan ke stock_list"),
            })),
            Err(message) => Ok(Response::new(GetStockListFromInvezgoResponse {
                success: false,
                message,
            })),
        }
    }

    async fn get_stock_list_from_scylla(
        &self,
        _request: Request<GetStockListFromScyllaRequest>,
    ) -> Result<Response<GetStockListFromScyllaResponse>, Status> {
        let rows = crate::repository::token_ring_scan(self.session.as_ref())
            .await
            .map_err(Status::internal)?;

        let items = rows.into_iter().map(Self::db_row_to_proto).collect();

        Ok(Response::new(GetStockListFromScyllaResponse { items }))
    }
}
