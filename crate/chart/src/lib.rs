pub mod pb {
    tonic::include_proto!("chart");
}

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("chart_descriptor");

mod invezgo;
mod service;

pub use pb::chart_server::{Chart, ChartServer};
pub use service::ChartService;
