use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::pb::stock_list_server::StockList;
use crate::pb::{GetStockListRequest, GetStockListResponse};

pub struct StockListService {
    session: Arc<Session>,
}

impl StockListService {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_stock_list(
        &self,
        _request: Request<GetStockListRequest>,
    ) -> Result<Response<GetStockListResponse>, Status> {
        match crate::invezgo::fetch_and_save(self.session.clone()).await {
            Ok(count) => Ok(Response::new(GetStockListResponse {
                success: true,
                message: format!("{count} saham disimpan ke stock_list"),
            })),
            Err(message) => Ok(Response::new(GetStockListResponse {
                success: false,
                message,
            })),
        }
    }
}
