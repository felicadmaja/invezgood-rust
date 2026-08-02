pub mod pb {
    tonic::include_proto!("bandarmology");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("bandarmology_descriptor");

mod model;
mod repository;
mod service;

pub use model::{
    agg_key, BandarmologyDayDb, BandarmologyHarianRow, BandarmologyRow, KEYSPACE, TABLE,
    TABLE_HARIAN, TABLE_PORTOFOLIO,
};

pub use pb::bandarmology_server::{Bandarmology, BandarmologyServer};
pub use service::BandarmologyService;
