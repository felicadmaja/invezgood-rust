//! Crate gRPC `realtime_price` — stream harga dari Stockbit `emitten/{CODE}/info`.

mod fetch;
mod hours;
mod hub;
mod redis_cache;
mod service;

tonic::include_proto!("realtime_price");

/// Descriptor set untuk gRPC reflection — didaftarkan di `app` / bin server.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("realtime_price_descriptor");

pub use service::RealtimePriceService;
