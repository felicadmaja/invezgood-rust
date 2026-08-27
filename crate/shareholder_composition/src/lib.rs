pub mod pb {
    tonic::include_proto!("shareholder_composition");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("shareholder_composition_descriptor");

mod invezgo;
pub mod model;
mod repository;
mod service;

pub use model::{
    ShareholderCompositionPkRow, ShareholderCompositionRow, KEYSPACE, MV_BY_CODE, TABLE,
};
pub use pb::shareholder_composition_server::{
    ShareholderComposition, ShareholderCompositionServer,
};
pub use service::ShareholderCompositionService;
