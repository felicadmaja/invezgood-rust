//! Crate gRPC `portofolio` — baca data dari ScyllaDB `stockbit.portofolio`.

mod database;
pub mod model;
mod repository;
mod service;

pub use database::session;

tonic::include_proto!("portofolio");

pub use service::PortofolioService;
