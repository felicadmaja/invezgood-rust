//! Model baris tabel `invezgood.top_gainer_loser`.

use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "top_gainer_loser";

/// Satu titik grafik (`date`, `value`) — maps UDT `top_gainer_loser_graph_point`.
pub type GraphPoint = (String, f64);

/// Satu baris `invezgood.top_gainer_loser`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct TopGainerLoserRow {
    pub tahun_bulan_tanggal: chrono::NaiveDate,
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub price: Option<f64>,
    #[scylla(rename = "change_pct")]
    #[scylla(default_when_null)]
    pub change_pct: Option<f64>,
    #[scylla(default_when_null)]
    pub value: Option<String>,
    #[scylla(default_when_null)]
    pub volume: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub calculated_value: Option<f64>,
    #[scylla(default_when_null)]
    pub tipe: Option<String>,
    #[scylla(default_when_null)]
    pub graph: Option<Vec<GraphPoint>>,
}
