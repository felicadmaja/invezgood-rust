pub mod pb {
    tonic::include_proto!("msci");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("msci_descriptor");

mod model;
mod repository;
mod service;

pub use model::{MsciRow, KEYSPACE, TABLE};
pub use pb::msci_server::{Msci, MsciServer};
pub use service::MsciService;
