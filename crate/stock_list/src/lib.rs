pub mod pb {
    tonic::include_proto!("stock_list");
}

/// File descriptor set untuk gRPC reflection (`grpcurl -plaintext localhost:50054 list`).
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("stock_list_descriptor");

mod database;
mod invezgo;
mod model;
mod repository;
mod service;

pub use database::connect;
pub use model::{StockListRow, KEYSPACE, TABLE};
pub use pb::stock_list_server::{StockList, StockListServer};
pub use service::StockListService;
