use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PendingOrder;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

const COLUMNS: &str = "order_id, emiten_name, status, message, side, time_open, \
    lot_open, lot_done, price_order, amount_open, amount_match, amount_match_total, \
    is_gtc, updated_at";

struct Prepared {
    scan: PreparedStatement,
}

pub struct PendingOrderRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl PendingOrderRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.pending_order"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(order_id) >= ? AND token(order_id) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { scan })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Semua baris `pending_order` (semua status) via token-ring scan pada PK `order_id`.
    pub async fn get_all(
        &self,
    ) -> Result<Vec<PendingOrder>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let stmt = prepared.scan.clone();

        let segment_rows: Vec<Vec<PendingOrder>> = stream::iter(0..TOKEN_SEGMENTS)
            .map(|seg| {
                let session = Arc::clone(&self.session);
                let stmt = stmt.clone();
                let start = token_segment_start(seg, TOKEN_SEGMENTS);
                let end = token_segment_end(seg, TOKEN_SEGMENTS);
                async move {
                    let pager = session.execute_iter(stmt, (start, end)).await?;
                    let mut rows = pager.rows_stream::<PendingOrder>()?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await {
                        out.push(row?);
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(out)
                }
            })
            .buffer_unordered(SCAN_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(segment_rows.into_iter().flatten().collect())
    }
}

fn token_segment_start(seg: usize, num_seg: usize) -> i64 {
    if seg == 0 {
        i64::MIN
    } else {
        let span = (i64::MAX as i128) - (i64::MIN as i128);
        (i64::MIN as i128 + (span * seg as i128) / num_seg as i128) as i64
    }
}

fn token_segment_end(seg: usize, num_seg: usize) -> i64 {
    if seg + 1 == num_seg {
        i64::MAX
    } else {
        token_segment_start(seg + 1, num_seg).saturating_sub(1)
    }
}
