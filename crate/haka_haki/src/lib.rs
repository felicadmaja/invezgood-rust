pub mod pb {
    tonic::include_proto!("haka_haki");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("haka_haki_descriptor");

mod invezgo;
mod intraday_cache;
mod model;
mod redis_cache;
mod repository;
mod service;

pub use intraday_cache::{new_shared_intraday_cache, IntradayCache};
pub use model::{
    agg_code_tahun_bulan_tanggal, HakaHakiRow, KEYSPACE, MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL, TABLE,
};
pub use pb::haka_haki_server::{HakaHaki, HakaHakiServer};
pub use service::HakaHakiService;
