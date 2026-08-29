//! Crate gRPC `config_fundamental` — baca ScyllaDB `invezgood.config_fundamental`.

pub mod pb {
    tonic::include_proto!("config_fundamental");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("config_fundamental_descriptor");

pub mod model;
mod repository;
mod service;

pub use model::{ConfigFundamentalRow, KEYSPACE, TABLE};
pub use pb::config_fundamental_server::{ConfigFundamental, ConfigFundamentalServer};
pub use service::ConfigFundamentalService;
