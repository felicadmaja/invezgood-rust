use std::sync::Arc;

use chrono::NaiveDate;
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{EmitenTrending, KEYSPACE, MV_BY_DATE};

const MV_BY_DATE_Q: &str = "SELECT agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, \
    gainer_or_loser, emiten_name, long_name, emiten_icon, sector, price, \
    price_change, value, volume, freq, updated_at \
    FROM invezgood.emiten_trending_by_tahun_bulan_tanggal WHERE tahun_bulan_tanggal = ?";

pub struct EmitenTrendingRepository {
    session: Arc<Session>,
}

impl EmitenTrendingRepository {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    pub async fn get_all_by_date(&self, date: NaiveDate) -> Result<Vec<EmitenTrending>, String> {
        let mut rows = self
            .session
            .query_iter(MV_BY_DATE_Q, (date,))
            .await
            .map_err(|e| format!("MV {KEYSPACE}.{MV_BY_DATE} date={date}: {e}"))?
            .rows_stream::<EmitenTrending>()
            .map_err(|e| format!("MV stream {KEYSPACE}.{MV_BY_DATE}: {e}"))?;

        let mut out = Vec::new();
        while let Some(item) = rows
            .try_next()
            .await
            .map_err(|e| format!("MV row {KEYSPACE}.{MV_BY_DATE}: {e}"))?
        {
            out.push(item);
        }

        Ok(out)
    }
}
