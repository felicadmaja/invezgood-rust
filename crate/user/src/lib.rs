pub mod pb {
    tonic::include_proto!("user");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("user_descriptor");

mod auth;
mod model;
pub mod password;
mod repository;
mod service;

pub use auth::{extract_bearer_token, new_session_store, validate_session, AuthSession, SessionStore};
pub use pb::user_server::{User, UserServer};
pub use service::UserService;
