pub mod pb {
    tonic::include_proto!("portofolio_history");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_history_descriptor");

mod database;
pub mod model;
mod repository;
mod service;
mod stockbit_cache;

pub use model::{PortofolioHistory, PortofolioHistoryItem, KEYSPACE, TABLE};
pub use pb::portofolio_history_server::{PortofolioHistory as PortofolioHistoryRpc, PortofolioHistoryServer};
pub use service::PortofolioHistoryService;
