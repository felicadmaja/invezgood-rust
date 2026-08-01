use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::pb::stock_list_server::StockList;
use crate::pb::{
    GetStockListFromInvezgoRequest, GetStockListFromInvezgoResponse, ListRequest, ListResponse,
    StockItem,
};

pub struct StockListService {
    session: Arc<Session>,
}

impl StockListService {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn row_to_item(row: crate::StockListRow) -> StockItem {
        StockItem {
            code: row.code,
            name: row.name.unwrap_or_default(),
            sector: row.sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        let limit = request.into_inner().limit.clamp(1, 1000) as i32;

        let rows = crate::repository::list(self.session.as_ref(), limit)
            .await
            .map_err(|e| Status::internal(e))?;

        let items = rows.into_iter().map(Self::row_to_item).collect();

        Ok(Response::new(ListResponse { items }))
    }

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
}
