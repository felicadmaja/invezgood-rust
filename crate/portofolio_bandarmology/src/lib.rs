//! Crate gRPC `portofolio_bandarmology` — ScyllaDB `stockbit.portofolio_bandarmology`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("portofolio_bandarmology");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_bandarmology_descriptor");

pub use service::PortofolioBandarmologyService;
