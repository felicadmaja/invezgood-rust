pub mod pb {
    tonic::include_proto!("portofolio");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_descriptor");

mod model;
mod repository;
mod service;

pub use model::{PortofolioRow, KEYSPACE, TABLE};

pub use pb::portofolio_server::{Portofolio, PortofolioServer};
pub use service::PortofolioService;
