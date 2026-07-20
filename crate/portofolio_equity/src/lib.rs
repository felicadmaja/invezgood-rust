//! Crate gRPC `portofolio_equity` — baca data dari ScyllaDB `stockbit.portofolio_equity`.

mod database;
pub mod model;
mod repository;
mod service;

pub use database::session;

tonic::include_proto!("portofolio_equity");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("portofolio_equity_descriptor");

pub use service::PortofolioEquityService;
