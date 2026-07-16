//! Crate gRPC `emiten_trending` — baca data dari ScyllaDB `stockbit.emiten_trending`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("emiten_trending");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("emiten_trending_descriptor");

pub use service::EmitenTrendingService;
