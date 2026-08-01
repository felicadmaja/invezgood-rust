use tonic::{Request, Response, Status};

use crate::pb::stock_list_server::StockList;
use crate::pb::{ListRequest, ListResponse, StockItem};

pub struct StockListService;

#[tonic::async_trait]
impl StockList for StockListService {
    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        let limit = request.into_inner().limit.clamp(1, 100) as usize;

        let items: Vec<StockItem> = [
            StockItem {
                symbol: "BBCA".into(),
                name: "Bank Central Asia".into(),
            },
            StockItem {
                symbol: "BBRI".into(),
                name: "Bank Rakyat Indonesia".into(),
            },
            StockItem {
                symbol: "BMRI".into(),
                name: "Bank Mandiri".into(),
            },
        ]
        .into_iter()
        .take(limit)
        .collect();

        Ok(Response::new(ListResponse { items }))
    }
}
