//! Model baris tabel `invezgood.portofolio` dan `invezgood.portofolio_equity`.

use scylla::DeserializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "portofolio";
pub const EQUITY_TABLE: &str = "portofolio_equity";

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

/// Satu baris `invezgood.portofolio_equity`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct PortofolioEquityRow {
    #[scylla(default_when_null)]
    pub nama: String,
    #[scylla(default_when_null)]
    pub value: f64,
}
