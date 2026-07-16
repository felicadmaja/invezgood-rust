use uuid::Uuid;

use crate::PortofolioRow;

#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct Portofolio {
    pub id: Uuid,
    #[scylla(default_when_null)]
    pub emiten_name: String,
    pub balance_lot: i64,
    pub available_lot: i64,
    pub average_price: f64,
    pub current_price: f64,
    pub invested: f64,
    pub market_value: f64,
    pub potential_p_l: f64,
    pub percentage: f64,
}

impl Portofolio {
    pub fn into_proto(self) -> PortofolioRow {
        PortofolioRow {
            id: self.id.to_string(),
            emiten_name: self.emiten_name,
            balance_lot: self.balance_lot,
            available_lot: self.available_lot,
            average_price: self.average_price,
            current_price: self.current_price,
            invested: self.invested,
            market_value: self.market_value,
            potential_p_l: self.potential_p_l,
            percentage: self.percentage,
        }
    }
}
