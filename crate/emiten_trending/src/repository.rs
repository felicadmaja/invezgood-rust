use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::{EmitenTrending, EmitenTrendingByTahunBulanTanggal};

struct Prepared {
    mv_by_date: PreparedStatement,
    base_by_agg: PreparedStatement,
}

pub struct EmitenTrendingRepository {
    session: Arc<Session>,
    mv_table: String,
    base_table: String,
    prepared: OnceCell<Prepared>,
}

impl EmitenTrendingRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            mv_table: format!("{ks}.emiten_trending_by_tahun_bulan_tanggal"),
            base_table: format!("{ks}.emiten_trending"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let mv_q = format!(
                    "SELECT tahun_bulan_tanggal, agg_tahun_bulan_tanggal_emiten_name \
                     FROM {} WHERE tahun_bulan_tanggal = ?",
                    self.mv_table
                );
                let base_q = format!(
                    "SELECT agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, \
                     gainer_or_loser, emiten_name, long_name, emiten_icon, price, price_change, \
                     value, volume, freq, updated_at \
                     FROM {} WHERE agg_tahun_bulan_tanggal_emiten_name = ?",
                    self.base_table
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    mv_by_date: self.session.prepare(mv_q).await?,
                    base_by_agg: self.session.prepare(base_q).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn get_all_by_date(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<EmitenTrending>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;

        let mv_rows = self
            .session
            .execute_unpaged(&prepared.mv_by_date, (date,))
            .await?
            .into_rows_result()?;

        let mut out = Vec::new();
        for row in mv_rows.rows::<EmitenTrendingByTahunBulanTanggal>()? {
            let agg = row?.agg_tahun_bulan_tanggal_emiten_name;
            let base = self
                .session
                .execute_unpaged(&prepared.base_by_agg, (agg.as_str(),))
                .await?
                .into_rows_result()?;
            if let Some(item) = base.maybe_first_row::<EmitenTrending>()? {
                out.push(item);
            }
        }

        Ok(out)
    }
}
