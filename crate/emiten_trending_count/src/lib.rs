//! Crate gRPC `emiten_trending_count` — baca data dari ScyllaDB
//! `stockbit.emiten_trending_count_by_name`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("emiten_trending_count");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("emiten_trending_count_descriptor");

pub use service::EmitenTrendingCountService;
