//! Crate gRPC `portofolio` — baca data dari ScyllaDB `stockbit.portofolio`.

mod database;
pub mod model;
mod repository;
mod service;

pub use database::session;

tonic::include_proto!("portofolio");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_descriptor");

pub use service::PortofolioService;
