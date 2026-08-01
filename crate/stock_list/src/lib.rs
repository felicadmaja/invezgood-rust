pub mod pb {
    tonic::include_proto!("stock_list");
}

mod database;
mod invezgo;
mod model;
mod repository;
mod service;

pub use database::connect;
pub use model::{StockListRow, KEYSPACE, TABLE};
pub use pb::stock_list_server::{StockList, StockListServer};
pub use service::StockListService;
