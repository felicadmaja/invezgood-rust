//! Crate gRPC `portofolio_history` — ScyllaDB `stockbit.portofolio_history`.

mod database;
pub mod model;
mod redis_cache;
mod repository;
mod service;

tonic::include_proto!("portofolio_history");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_history_descriptor");

pub use service::PortofolioHistoryService;
