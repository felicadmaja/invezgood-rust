//! Crate `user` — gRPC Login + JWT untuk autentikasi antar-crate.
//!
//! Crate lain: pakai [`AuthInterceptor`] / [`require_auth`] / [`take_claims`].

mod database;
mod jwt;
mod model;
mod repository;
mod service;

pub mod auth;

tonic::include_proto!("user");

pub use auth::{require_auth, take_claims, AuthInterceptor};
pub use database::session;
pub use jwt::Claims;
pub use service::UserService;
