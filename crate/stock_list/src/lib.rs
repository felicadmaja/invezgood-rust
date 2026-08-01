pub mod pb {
    tonic::include_proto!("stock_list");
}

mod model;
mod service;

pub use model::{StockListRow, KEYSPACE, TABLE};

pub use pb::stock_list_server::{StockList, StockListServer};
pub use service::StockListService;
