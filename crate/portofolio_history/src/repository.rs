use std::sync::Arc;

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::{PortofolioHistory, KEYSPACE, MV_BY_TAHUN_BULAN};

struct Prepared {
    latest_by_emiten: PreparedStatement,
}

pub struct PortofolioHistoryRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl PortofolioHistoryRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.portofolio_history"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let latest = format!(
                    "SELECT emiten_name, tahun_bulan_tanggal, tahun_bulan, history \
                     FROM {} WHERE emiten_name = ? LIMIT 1",
                    self.table
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    latest_by_emiten: self.session.prepare(latest).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn find_latest_by_emiten(
        &self,
        emiten_name: &str,
    ) -> Result<Option<PortofolioHistory>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.latest_by_emiten, (emiten_name,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<PortofolioHistory>()?)
    }

    pub async fn find_by_tahun_bulan(
        &self,
        tahun_bulan: &str,
    ) -> Result<Vec<PortofolioHistory>, Box<dyn std::error::Error + Send + Sync>> {
        let q = format!(
            "SELECT emiten_name, tahun_bulan_tanggal, tahun_bulan, history \
             FROM {KEYSPACE}.{MV_BY_TAHUN_BULAN} WHERE tahun_bulan = ?"
        );
        let mut rows = self
            .session
            .query_iter(q.as_str(), (tahun_bulan,))
            .await?
            .rows_stream::<PortofolioHistory>()?;

        let mut out = Vec::new();
        while let Some(row) = rows.try_next().await? {
            out.push(row);
        }
        Ok(out)
    }
}
