use std::sync::Arc;

use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::{agg_tahun_bulan_emiten_name, Bandarmology};

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
                    "SELECT agg_tahun_bulan_emiten_name, emiten_name, tahun_bulan, \
                     broker_summary_current_w1, broker_summary_current_w2, \
                     broker_summary_current_w3, broker_summary_current_w4, \
                     broker_summary, updated_at \
                     FROM {} WHERE agg_tahun_bulan_emiten_name = ? LIMIT 1",
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

    pub async fn find_by_tahun_bulan_and_emiten(
        &self,
        tahun_bulan: &str,
        kode_emiten: &str,
    ) -> Result<Option<Bandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let agg = agg_tahun_bulan_emiten_name(tahun_bulan, kode_emiten);
        let result = self
            .session
            .execute_unpaged(&prepared.by_agg, (agg.as_str(),))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<Bandarmology>()?)
    }
}
