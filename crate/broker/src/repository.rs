use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::Broker;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

struct Prepared {
    scan: PreparedStatement,
    by_broker_code: PreparedStatement,
    insert: PreparedStatement,
    update: PreparedStatement,
    delete: PreparedStatement,
}

pub struct BrokerRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl BrokerRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.broker"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                const COLUMNS: &str = "broker_code, name, tipe, asosiasi, catatan";
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(broker_code) >= ? AND token(broker_code) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);

                let q = format!("SELECT {COLUMNS} FROM {} WHERE broker_code = ?", self.table);
                let by_broker_code = self.session.prepare(q).await?;

                let q = format!(
                    "INSERT INTO {} (broker_code, name, tipe, asosiasi, catatan) \
                     VALUES (?, ?, ?, ?, ?)",
                    self.table
                );
                let insert = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET name = ?, tipe = ?, asosiasi = ?, catatan = ? \
                     WHERE broker_code = ?",
                    self.table
                );
                let update = self.session.prepare(q).await?;

                let q = format!(
                    "DELETE FROM {} WHERE broker_code = ?",
                    self.table
                );
                let delete = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    scan,
                    by_broker_code,
                    insert,
                    update,
                    delete,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Token-ring scan seluruh partisi `broker`.
    pub async fn get_all(&self) -> Result<Vec<Broker>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let stmt = prepared.scan.clone();

        let segment_rows: Vec<Vec<Broker>> = stream::iter(0..TOKEN_SEGMENTS)
            .map(|seg| {
                let session = Arc::clone(&self.session);
                let stmt = stmt.clone();
                let start = token_segment_start(seg, TOKEN_SEGMENTS);
                let end = token_segment_end(seg, TOKEN_SEGMENTS);
                async move {
                    let pager = session.execute_iter(stmt, (start, end)).await?;
                    let mut rows = pager.rows_stream::<Broker>()?;
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

    /// Lookup satu baris via PK: `WHERE broker_code = ?`.
    pub async fn get_by_broker_code(
        &self,
        broker_code: &str,
    ) -> Result<Option<Broker>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.by_broker_code, (broker_code,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<Broker>()?)
    }

    pub async fn exists_by_broker_code(
        &self,
        broker_code: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.get_by_broker_code(broker_code).await?.is_some())
    }

    /// Insert baris baru. Mengembalikan `Ok(false)` bila `broker_code` sudah ada.
    pub async fn insert(
        &self,
        broker_code: &str,
        name: &str,
        tipe: &str,
        asosiasi: &str,
        catatan: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.exists_by_broker_code(broker_code).await? {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.insert,
                (broker_code, name, tipe, asosiasi, catatan),
            )
            .await?;
        Ok(true)
    }

    /// Update baris existing. Mengembalikan `Ok(false)` bila `broker_code` tidak ada.
    pub async fn update(
        &self,
        broker_code: &str,
        name: &str,
        tipe: &str,
        asosiasi: &str,
        catatan: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if !self.exists_by_broker_code(broker_code).await? {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update,
                (name, tipe, asosiasi, catatan, broker_code),
            )
            .await?;
        Ok(true)
    }

    /// Hapus baris. Mengembalikan `Ok(false)` bila `broker_code` tidak ada.
    pub async fn delete(&self, broker_code: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if !self.exists_by_broker_code(broker_code).await? {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.delete, (broker_code,))
            .await?;
        Ok(true)
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
