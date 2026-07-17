use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::EmitenList;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

struct Prepared {
    scan: PreparedStatement,
    by_code_name: PreparedStatement,
}

pub struct EmitenListRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl EmitenListRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.emiten_list"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                const COLUMNS: &str = "code_name, long_name, emiten_icon, key_stats, corporate_action, \
                     company_profile, update_at, is_konglomerasi";
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(code_name) >= ? AND token(code_name) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);

                let q = format!("SELECT {COLUMNS} FROM {} WHERE code_name = ?", self.table);
                let by_code_name = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { scan, by_code_name })
            })
            .await
    }

    /// Preflight cache prepared statements — wajib di-await di binary utama sebelum serve.
    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Token-ring scan seluruh partisi `emiten_list`.
    pub async fn get_all(&self) -> Result<Vec<EmitenList>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let stmt = prepared.scan.clone();

        let segment_rows: Vec<Vec<EmitenList>> = stream::iter(0..TOKEN_SEGMENTS)
            .map(|seg| {
                let session = Arc::clone(&self.session);
                let stmt = stmt.clone();
                let start = token_segment_start(seg, TOKEN_SEGMENTS);
                let end = token_segment_end(seg, TOKEN_SEGMENTS);
                async move {
                    let pager = session.execute_iter(stmt, (start, end)).await?;
                    let mut rows = pager.rows_stream::<EmitenList>()?;
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

    /// Lookup satu baris via PK base table: `WHERE code_name = ?`.
    pub async fn get_by_code_name(
        &self,
        code_name: &str,
    ) -> Result<Option<EmitenList>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.by_code_name, (code_name,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<EmitenList>()?)
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
