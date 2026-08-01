pub mod pb {
    tonic::include_proto!("user");
}

/// File descriptor set untuk gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("user_descriptor");

mod auth;
mod model;
mod repository;
mod service;

pub use model::{UserRow, KEYSPACE, TABLE};
pub use pb::user_server::{User, UserServer};
pub use service::UserService;
