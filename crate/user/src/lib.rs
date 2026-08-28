pub mod pb {
    tonic::include_proto!("user");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("user_descriptor");

mod auth;
mod invezgo;
mod model;
pub mod password;
mod repository;
mod service;

pub use auth::{
    extract_bearer_token, jwt_expiry_secs, new_session_store, require_stockbit_scrape_hours,
    validate_session, AuthSession, SessionStore, DEFAULT_JWT_EXPIRY_SECS,
};
pub use pb::user_server::{User, UserServer};
pub use service::UserService;
