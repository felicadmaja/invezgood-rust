//! Crate gRPC `emiten_list` — baca data dari ScyllaDB `stockbit.emiten_list`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("emiten_list");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("emiten_list_descriptor");

pub use service::EmitenListService;
