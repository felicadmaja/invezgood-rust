use std::sync::Arc;

use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{PortofolioEquity, KEYSPACE, TABLE};

const FIND_ALL: &str = "SELECT nama, value FROM invezgood.portofolio_equity";

#[derive(Clone)]
pub struct PortofolioEquityRepository {
    session: Arc<Session>,
}

impl PortofolioEquityRepository {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    /// Semua baris `portofolio_equity` (tabel kecil, ~5 metrik).
    pub async fn get_all(&self) -> Result<Vec<PortofolioEquity>, String> {
        let mut rows = self
            .session
            .query_iter(FIND_ALL, &[])
            .await
            .map_err(|e| format!("get_all {KEYSPACE}.{TABLE}: {e}"))?
            .rows_stream::<PortofolioEquity>()
            .map_err(|e| format!("get_all stream {KEYSPACE}.{TABLE}: {e}"))?;

        let mut items = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| format!("get_all row {KEYSPACE}.{TABLE}: {e}"))?
        {
            items.push(row);
        }
        Ok(items)
    }
}
