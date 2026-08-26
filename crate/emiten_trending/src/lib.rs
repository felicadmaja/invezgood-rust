pub mod pb {
    tonic::include_proto!("emiten_trending");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("emiten_trending_descriptor");

mod invezgo;
mod model;
mod repository;
mod service;

pub use model::{
    agg_tahun_bulan_tanggal_emiten_name, EmitenTrending, KEYSPACE, MV_BY_DATE, MV_BY_EMITEN, TABLE,
};
pub use pb::emiten_trending_server::{EmitenTrending as EmitenTrendingRpc, EmitenTrendingServer};
pub use service::EmitenTrendingService;
