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
    update_fundamental_solid: PreparedStatement,
    update_sector: PreparedStatement,
    update_konglomerasi: PreparedStatement,
    update_blue_chip: PreparedStatement,
    update_plan_to_trade: PreparedStatement,
    update_catatan: PreparedStatement,
    update_catatan_owner: PreparedStatement,
    update_foto_owner: PreparedStatement,
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
                     company_profile, update_at, is_konglomerasi, sector, is_fundamental_solid, is_blue_chip, \
                     is_plan_to_trade, catatan, catatan_owner, foto_owner, net_income";
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(code_name) >= ? AND token(code_name) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);

                let q = format!("SELECT {COLUMNS} FROM {} WHERE code_name = ?", self.table);
                let by_code_name = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_fundamental_solid = ? WHERE code_name = ?",
                    self.table
                );
                let update_fundamental_solid = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET sector = ? WHERE code_name = ?",
                    self.table
                );
                let update_sector = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_konglomerasi = ? WHERE code_name = ?",
                    self.table
                );
                let update_konglomerasi = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_blue_chip = ? WHERE code_name = ?",
                    self.table
                );
                let update_blue_chip = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_plan_to_trade = ? WHERE code_name = ?",
                    self.table
                );
                let update_plan_to_trade = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan = ? WHERE code_name = ?",
                    self.table
                );
                let update_catatan = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan_owner = ? WHERE code_name = ?",
                    self.table
                );
                let update_catatan_owner = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET foto_owner = ? WHERE code_name = ?",
                    self.table
                );
                let update_foto_owner = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    scan,
                    by_code_name,
                    update_fundamental_solid,
                    update_sector,
                    update_konglomerasi,
                    update_blue_chip,
                    update_plan_to_trade,
                    update_catatan,
                    update_catatan_owner,
                    update_foto_owner,
                })
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

    /// Update `is_fundamental_solid`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_fundamental_solid(
        &self,
        code_name: &str,
        is_fundamental_solid: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_fundamental_solid,
                (is_fundamental_solid, code_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `sector` (tinyint). Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_sector(
        &self,
        code_name: &str,
        sector: i8,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_sector, (sector, code_name))
            .await?;
        Ok(true)
    }

    /// Update `is_konglomerasi`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_konglomerasi(
        &self,
        code_name: &str,
        is_konglomerasi: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_konglomerasi,
                (is_konglomerasi, code_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `is_blue_chip`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_blue_chip(
        &self,
        code_name: &str,
        is_blue_chip: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_blue_chip, (is_blue_chip, code_name))
            .await?;
        Ok(true)
    }

    /// Update `is_plan_to_trade`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_plan_to_trade(
        &self,
        code_name: &str,
        is_plan_to_trade: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_plan_to_trade,
                (is_plan_to_trade, code_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `catatan`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_catatan(
        &self,
        code_name: &str,
        catatan: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_catatan, (catatan, code_name))
            .await?;
        Ok(true)
    }

    /// Update `catatan_owner`. Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_catatan_owner(
        &self,
        code_name: &str,
        catatan_owner: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_catatan_owner,
                (catatan_owner, code_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `foto_owner` (list<path>). Mengembalikan `Ok(false)` bila `code_name` tidak ada.
    pub async fn update_foto_owner(
        &self,
        code_name: &str,
        foto_owner: &[String],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_code_name(code_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_foto_owner, (foto_owner, code_name))
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
