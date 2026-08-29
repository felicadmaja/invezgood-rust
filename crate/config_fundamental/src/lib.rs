pub mod pb {
    tonic::include_proto!("config_fundamental");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("config_fundamental_descriptor");

pub mod model;

pub use model::{ConfigFundamentalRow, KEYSPACE, TABLE};
