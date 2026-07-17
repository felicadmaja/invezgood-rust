//! Crate gRPC `gcs` — signed upload/preview Google Cloud Storage (modul `stoksaham`).

pub mod client;
mod service;

tonic::include_proto!("gcs");

/// Descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("gcs_descriptor");

pub use client::{
    download_and_upload_emiten_icon, emiten_icon_object_path, gcs_runtime, gcs_upload_bytes,
    load_gcs_signed_url_runtime, GcsOAuthTokenCache, GcsSignedUrlRuntime, MODULE_STOKSAHAM,
};
pub use service::GcsGrpcService;
