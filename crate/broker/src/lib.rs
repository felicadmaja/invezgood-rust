pub mod pb {
    tonic::include_proto!("broker");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("broker_descriptor");

mod invezgo;
mod model;
mod repository;
mod service;

pub use model::{BrokerRow, KEYSPACE, TABLE};

pub use pb::broker_server::{Broker, BrokerServer};
pub use service::BrokerService;
