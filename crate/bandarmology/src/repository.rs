use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::{agg_tahun_bulan_tanggal_emiten_name, Bandarmology};

struct Prepared {
    by_agg: PreparedStatement,
}

pub struct BandarmologyRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl BandarmologyRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.bandarmology"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT agg_tahun_bulan_tanggal_emiten_name, emiten_name, tahun_bulan_tanggal, \
                     d_1, d_2, d_7, d_14, \"M_1\", \"M_3\", \"M_6\", \"M_12\", \
                     \"Y_3\", \"Y_5\", \"Y_10\", \"Y_15\" \
                     FROM {} WHERE agg_tahun_bulan_tanggal_emiten_name = ? LIMIT 1",
                    self.table
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    by_agg: self.session.prepare(q).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn find_by_date_and_emiten(
        &self,
        date: NaiveDate,
        kode_emiten: &str,
    ) -> Result<Option<Bandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let agg = agg_tahun_bulan_tanggal_emiten_name(date, kode_emiten);
        let result = self
            .session
            .execute_unpaged(&prepared.by_agg, (agg.as_str(),))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<Bandarmology>()?)
    }
}
