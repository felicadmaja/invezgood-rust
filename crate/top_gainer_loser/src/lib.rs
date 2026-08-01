pub mod pb {
    tonic::include_proto!("top_gainer_loser");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("top_gainer_loser_descriptor");

mod invezgo;
mod model;
mod repository;
mod service;

pub use model::{GraphPoint, TopGainerLoserRow, KEYSPACE, TABLE};

pub use pb::top_gainer_loser_server::{TopGainerLoser, TopGainerLoserServer};
pub use service::TopGainerLoserService;
