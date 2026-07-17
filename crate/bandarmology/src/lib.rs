//! Crate gRPC `bandarmology` — baca data dari ScyllaDB `stockbit.bandarmology`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("bandarmology");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("bandarmology_descriptor");

pub use service::BandarmologyService;
