pub mod pb {
    tonic::include_proto!("portofolio_equity");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_equity_descriptor");

mod model;
pub mod repository;
mod service;

pub use model::{PortofolioEquity, KEYSPACE, TABLE};
pub use pb::portofolio_equity_server::PortofolioEquityServer;
pub use service::PortofolioEquityService;
