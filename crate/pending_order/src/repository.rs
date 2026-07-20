use std::sync::Arc;

use futures_util::StreamExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PendingOrder;

const PAGE_SIZE: i32 = 100;
pub const STATUS_OPEN: &str = "OPEN";
pub const STATUS_MATCH: &str = "MATCH";
pub const STATUS_REJECTED: &str = "REJECTED";

const COLUMNS: &str = "order_id, emiten_name, status, message, side, time_open, \
    lot_open, lot_done, price_order, amount_open, amount_match, amount_match_total, \
    is_gtc, updated_at";

struct Prepared {
    by_status: PreparedStatement,
}

pub struct PendingOrderRepository {
    session: Arc<Session>,
    mv_by_status: String,
    prepared: OnceCell<Prepared>,
}

impl PendingOrderRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            mv_by_status: format!("{ks}.pending_order_by_status"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT {COLUMNS} FROM {} WHERE status = ?",
                    self.mv_by_status
                );
                let mut by_status = self.session.prepare(q).await?;
                by_status.set_page_size(PAGE_SIZE);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { by_status })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Pending order via MV `pending_order_by_status` untuk `status` tertentu.
    pub async fn get_all_by_status(
        &self,
        status: &str,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let pager = self
            .session
            .execute_iter(prepared.by_status.clone(), (status,))
            .await?;
        let mut rows = pager.rows_stream::<PendingOrder>()?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await {
            out.push(row?);
        }
        Ok(out)
    }

    pub async fn get_all_open(
        &self,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_all_by_status(STATUS_OPEN).await
    }

    pub async fn get_all_match(
        &self,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_all_by_status(STATUS_MATCH).await
    }

    pub async fn get_all_rejected(
        &self,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_all_by_status(STATUS_REJECTED).await
    }
}
