use std::sync::Arc;

use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::database::keyspace;
use crate::model::{PortofolioHistory, KEYSPACE, MV_BY_TAHUN_BULAN};

pub struct PortofolioHistoryRepository {
    session: Arc<Session>,
    table: String,
}

impl PortofolioHistoryRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.portofolio_history"),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    pub async fn find_all_by_emiten(
        &self,
        emiten_name: &str,
    ) -> Result<Vec<PortofolioHistory>, Box<dyn std::error::Error + Send + Sync>> {
        let q = format!(
            "SELECT emiten_name, tahun_bulan_tanggal, tahun_bulan, history \
             FROM {} WHERE emiten_name = ?",
            self.table
        );
        let mut rows = self
            .session
            .query_iter(q.as_str(), (emiten_name,))
            .await?
            .rows_stream::<PortofolioHistory>()?;

        let mut out = Vec::new();
        while let Some(row) = rows.try_next().await? {
            out.push(row);
        }
        Ok(out)
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
