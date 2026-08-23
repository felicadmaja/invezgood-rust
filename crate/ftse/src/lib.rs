pub mod pb {
    tonic::include_proto!("ftse");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ftse_descriptor");

mod model;
mod repository;
mod service;

pub use model::{FtseRow, KEYSPACE, TABLE};
pub use pb::ftse_server::{Ftse, FtseServer};
pub use service::FtseService;
