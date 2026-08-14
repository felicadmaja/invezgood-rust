//! Crate gRPC `wyckoff_glossary` — tulis/baca ScyllaDB `invezgood.wyckoff_glossary`.

mod database;
pub mod model;
mod repository;
mod service;

tonic::include_proto!("wyckoff_glossary");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("wyckoff_glossary_descriptor");

pub use service::WyckoffGlossaryService;
pub use wyckoff_glossary_server::{WyckoffGlossary, WyckoffGlossaryServer};
