//! Crate gRPC `portofolio_catatan` — tulis/baca ScyllaDB `stockbit.portofolio_catatan`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("portofolio_catatan");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_catatan_descriptor");

pub use service::PortofolioCatatanService;
