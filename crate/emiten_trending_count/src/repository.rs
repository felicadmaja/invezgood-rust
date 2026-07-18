use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::EmitenTrendingCountByName;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;
const TOP_N: usize = 10;
const MIN_APPEARANCE_COUNT: i64 = 10;

struct Prepared {
    scan: PreparedStatement,
}

pub struct EmitenTrendingCountRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl EmitenTrendingCountRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.emiten_trending_count_by_name"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT emiten_name, appearance_count, last_tahun_bulan_tanggal, updated_at \
                     FROM {} WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?",
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

    /// Token-ring scan seluruh baris, filter `appearance_count >= 10`,
    /// sort DESC, ambil top 10.
    pub async fn get_most_trending(
        &self,
    ) -> Result<Vec<EmitenTrendingCountByName>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let stmt = prepared.scan.clone();

        let segment_rows: Vec<Vec<EmitenTrendingCountByName>> = stream::iter(0..TOKEN_SEGMENTS)
            .map(|seg| {
                let session = Arc::clone(&self.session);
                let stmt = stmt.clone();
                let start = token_segment_start(seg, TOKEN_SEGMENTS);
                let end = token_segment_end(seg, TOKEN_SEGMENTS);
                async move {
                    let pager = session.execute_iter(stmt, (start, end)).await?;
                    let mut rows = pager.rows_stream::<EmitenTrendingCountByName>()?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await {
                        let row = row?;
                        if row.appearance_count >= MIN_APPEARANCE_COUNT {
                            out.push(row);
                        }
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(out)
                }
            })
            .buffer_unordered(SCAN_CONCURRENCY)
            .try_collect()
            .await?;

        let mut all: Vec<EmitenTrendingCountByName> =
            segment_rows.into_iter().flatten().collect();
        all.sort_by(|a, b| b.appearance_count.cmp(&a.appearance_count));
        all.truncate(TOP_N);
        Ok(all)
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
