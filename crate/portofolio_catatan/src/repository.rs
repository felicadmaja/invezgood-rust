use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PortofolioCatatan;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

struct Prepared {
    scan: PreparedStatement,
    by_emiten_name: PreparedStatement,
    insert: PreparedStatement,
    update: PreparedStatement,
    delete: PreparedStatement,
}

pub struct PortofolioCatatanRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl PortofolioCatatanRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.portofolio_catatan"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                const COLUMNS: &str = "emiten_name, catatan";
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);

                let q = format!(
                    "SELECT {COLUMNS} FROM {} WHERE emiten_name = ?",
                    self.table
                );
                let by_emiten_name = self.session.prepare(q).await?;

                let q = format!(
                    "INSERT INTO {} (emiten_name, catatan) VALUES (?, ?)",
                    self.table
                );
                let insert = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan = ? WHERE emiten_name = ?",
                    self.table
                );
                let update = self.session.prepare(q).await?;

                let q = format!("DELETE FROM {} WHERE emiten_name = ?", self.table);
                let delete = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    scan,
                    by_emiten_name,
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

    /// Token-ring scan seluruh partisi `portofolio_catatan`.
    pub async fn get_all(
        &self,
    ) -> Result<Vec<PortofolioCatatan>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let stmt = prepared.scan.clone();

        let segment_rows: Vec<Vec<PortofolioCatatan>> = stream::iter(0..TOKEN_SEGMENTS)
            .map(|seg| {
                let session = Arc::clone(&self.session);
                let stmt = stmt.clone();
                let start = token_segment_start(seg, TOKEN_SEGMENTS);
                let end = token_segment_end(seg, TOKEN_SEGMENTS);
                async move {
                    let pager = session.execute_iter(stmt, (start, end)).await?;
                    let mut rows = pager.rows_stream::<PortofolioCatatan>()?;
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

    pub async fn get_by_emiten_name(
        &self,
        emiten_name: &str,
    ) -> Result<Option<PortofolioCatatan>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.by_emiten_name, (emiten_name,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<PortofolioCatatan>()?)
    }

    /// Insert catatan baru. Mengembalikan `Ok(false)` bila `emiten_name` sudah ada.
    pub async fn insert(
        &self,
        emiten_name: &str,
        catatan: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_some() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.insert, (emiten_name, catatan))
            .await?;
        Ok(true)
    }

    /// Update catatan. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update(
        &self,
        emiten_name: &str,
        catatan: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update, (catatan, emiten_name))
            .await?;
        Ok(true)
    }

    /// Hapus catatan. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn delete(
        &self,
        emiten_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.delete, (emiten_name,))
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
