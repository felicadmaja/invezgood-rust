pub mod pb {
    tonic::include_proto!("bandarmology");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("bandarmology_descriptor");

mod invezgo;
mod model;
mod repository;
mod service;

pub use model::{BandarmologyEntryDb, BandarmologyRow, KEYSPACE, TABLE};

pub use pb::bandarmology_server::{Bandarmology, BandarmologyServer};
pub use service::BandarmologyService;
