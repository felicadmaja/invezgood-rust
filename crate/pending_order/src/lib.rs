//! Crate gRPC `pending_order` — baca data dari ScyllaDB `stockbit.pending_order`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("pending_order");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("pending_order_descriptor");

pub use service::PendingOrderService;
