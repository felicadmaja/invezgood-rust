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
mod sync;

pub use model::{TopForeignFlowPkRow, TopForeignFlowRow, KEYSPACE, MV_BY_CODE, MV_BY_TAHUN_BULAN_TANGGAL, TABLE};
pub use pb::top_foreign_flow_server::{TopForeignFlow, TopForeignFlowServer};
pub use service::TopForeignFlowService;
