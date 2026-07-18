//! Crate gRPC `broker` — baca data dari ScyllaDB `stockbit.broker`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("broker");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("broker_descriptor");

pub use service::BrokerService;
