use std::sync::Arc;

use futures_util::StreamExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PendingOrder;

const PAGE_SIZE: i32 = 100;
const STATUS_OPEN: &str = "OPEN";

const COLUMNS: &str = "order_id, emiten_name, status, message, side, time_open, \
    lot_open, lot_done, price_order, amount_open, amount_match, amount_match_total, \
    is_gtc, updated_at";

struct Prepared {
    by_status_open: PreparedStatement,
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
                let mut by_status_open = self.session.prepare(q).await?;
                by_status_open.set_page_size(PAGE_SIZE);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { by_status_open })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Semua pending order berstatus `OPEN` via MV `pending_order_by_status`.
    pub async fn get_all_open(
        &self,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let pager = self
            .session
            .execute_iter(prepared.by_status_open.clone(), (STATUS_OPEN,))
            .await?;
        let mut rows = pager.rows_stream::<PendingOrder>()?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await {
            out.push(row?);
        }
        Ok(out)
    }
}
