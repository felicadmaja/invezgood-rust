pub mod pb {
    tonic::include_proto!("stock_list");
}

mod service;

pub use pb::stock_list_server::{StockList, StockListServer};
pub use service::StockListService;
