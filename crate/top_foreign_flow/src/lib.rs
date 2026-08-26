pub mod pb {
    tonic::include_proto!("top_foreign_flow");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("top_foreign_flow_descriptor");

mod invezgo;
mod model;
mod repository;
mod service;

pub use model::{TopForeignFlowRow, KEYSPACE, TABLE};
pub use pb::top_foreign_flow_server::{TopForeignFlow, TopForeignFlowServer};
pub use service::TopForeignFlowService;
