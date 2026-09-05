pub mod pb {
    tonic::include_proto!("stock_list");
}

/// File descriptor set untuk gRPC reflection (`grpcurl -plaintext localhost:50054 list`).
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("stock_list_descriptor");

mod all_stocks_cache;
mod database;
mod invezgo;
mod model;
mod notation_scheduler;
mod redis_cache;
mod repository;
mod service;
mod stockbit;
pub mod stockbit_profile;
mod stockbit_reports;

pub use database::connect;
pub use notation_scheduler::spawn_daily_notation_sync;
pub use model::{StockListRow, KEYSPACE, MV_BY_IS_PLAN_TO_TRADE, TABLE};
pub use pb::stock_list_server::{StockList, StockListServer};
pub use service::StockListService;
