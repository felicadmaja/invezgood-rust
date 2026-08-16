//! Crate gRPC `hari_libur` — baca kalender libur nasional dari ScyllaDB `invezgood.hari_libur`.

pub mod pb {
    tonic::include_proto!("hari_libur");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("hari_libur_descriptor");

pub mod model;
mod repository;
mod service;

pub use model::{HariLiburRow, KEYSPACE, TABLE, VIEW_BY_TAHUN};

pub use pb::hari_libur_server::{HariLibur, HariLiburServer};
pub use service::HariLiburService;
