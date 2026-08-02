//! Model baris tabel `invezgood.portofolio`.

use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "portofolio";

/// Satu baris `invezgood.portofolio`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioRow {
    #[scylla(default_when_null)]
    pub emiten_name: String,
    #[scylla(default_when_null)]
    pub long_name: String,
    #[scylla(default_when_null)]
    pub emiten_icon: String,
    #[scylla(default_when_null)]
    pub balance_lot: i64,
    #[scylla(default_when_null)]
    pub available_lot: i64,
    #[scylla(default_when_null)]
    pub average_price: f64,
    #[scylla(default_when_null)]
    pub current_price: f64,
    #[scylla(default_when_null)]
    pub invested: f64,
    #[scylla(default_when_null)]
    pub market_value: f64,
    #[scylla(default_when_null)]
    pub potential_p_l: f64,
    #[scylla(default_when_null)]
    pub percentage: f64,
}
