use std::sync::Arc;

use chrono::NaiveDate;
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{
    EmitenTrending, EmitenTrendingByTahunBulanTanggal, KEYSPACE, MV_BY_DATE, TABLE,
};

const MV_BY_DATE_Q: &str = "SELECT tahun_bulan_tanggal, agg_tahun_bulan_tanggal_emiten_name \
    FROM invezgood.emiten_trending_by_tahun_bulan_tanggal WHERE tahun_bulan_tanggal = ?";

const BASE_BY_AGG: &str = "SELECT agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, \
    gainer_or_loser, emiten_name, long_name, emiten_icon, sector, price, \
    price_change, value, volume, freq, updated_at \
    FROM invezgood.emiten_trending WHERE agg_tahun_bulan_tanggal_emiten_name = ?";

pub struct EmitenTrendingRepository {
    session: Arc<Session>,
}

impl EmitenTrendingRepository {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    pub async fn get_all_by_date(&self, date: NaiveDate) -> Result<Vec<EmitenTrending>, String> {
        let mut mv_rows = self
            .session
            .query_iter(MV_BY_DATE_Q, (date,))
            .await
            .map_err(|e| format!("MV {KEYSPACE}.{MV_BY_DATE} date={date}: {e}"))?
            .rows_stream::<EmitenTrendingByTahunBulanTanggal>()
            .map_err(|e| format!("MV stream {KEYSPACE}.{MV_BY_DATE}: {e}"))?;

        let mut out = Vec::new();
        while let Some(mv) = mv_rows
            .try_next()
            .await
            .map_err(|e| format!("MV row {KEYSPACE}.{MV_BY_DATE}: {e}"))?
        {
            let agg = mv.agg_tahun_bulan_tanggal_emiten_name;
            let mut base = self
                .session
                .query_iter(BASE_BY_AGG, (agg.as_str(),))
                .await
                .map_err(|e| format!("select {KEYSPACE}.{TABLE} agg={agg}: {e}"))?
                .rows_stream::<EmitenTrending>()
                .map_err(|e| format!("select stream {KEYSPACE}.{TABLE}: {e}"))?;

            if let Some(item) = base
                .try_next()
                .await
                .map_err(|e| format!("select row {KEYSPACE}.{TABLE}: {e}"))?
            {
                out.push(item);
            }
        }

        Ok(out)
    }
}
