//! Crate gRPC `evtoebit` — median EV/EBIT per sektor/sub-sektor emiten BEI (Yahoo Finance, Rust).

pub mod pb {
    tonic::include_proto!("evtoebit");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("evtoebit_descriptor");

mod aggregate;
mod cache;
mod compute;
mod model;
pub mod repository;
mod scheduler;
mod service;
mod sync;
mod universe;
mod yahoo;

pub use model::{EvToEbitRow, KEYSPACE, TABLE};

pub use cache::{new_shared_median_cache, MedianCache};
pub use compute::{cache_ttl, compute_median};
pub use pb::ev_to_ebit_server::{EvToEbit, EvToEbitServer};
pub use scheduler::spawn_monthly_evtoebit_sync;
pub use service::EvToEbitService;
pub use sync::sync_median_from_yahoo_to_scylla;
pub use yahoo::YahooClient;

pub fn new_yahoo_client() -> Result<std::sync::Arc<YahooClient>, String> {
    YahooClient::new()
}
