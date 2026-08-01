use std::collections::HashMap;
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
    by_emiten_name: PreparedStatement,
    update_fundamental_solid: PreparedStatement,
    update_sector: PreparedStatement,
    update_konglomerasi: PreparedStatement,
    update_blue_chip: PreparedStatement,
    update_plan_to_trade: PreparedStatement,
    update_catatan: PreparedStatement,
    update_catatan_owner: PreparedStatement,
    update_catatan_pribadi: PreparedStatement,
    update_foto_owner: PreparedStatement,
    update_takeprofit_wyckoff: PreparedStatement,
    update_wyckoff_phase_element: PreparedStatement,
    update_wyckoff_trading_range: PreparedStatement,
    update_wyckoff_horizontal_line: PreparedStatement,
    trending_aggs_by_emiten: PreparedStatement,
    update_trending_sector: PreparedStatement,
}

pub struct EmitenListRepository {
    session: Arc<Session>,
    table: String,
    trending_mv: String,
    trending_table: String,
    prepared: OnceCell<Prepared>,
}

impl EmitenListRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.emiten_list"),
            trending_mv: format!("{ks}.emiten_trending_by_emiten_name"),
            trending_table: format!("{ks}.emiten_trending"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                const COLUMNS: &str = "emiten_name, long_name, emiten_icon, key_stats, corporate_action, \
                     company_profile, update_at, is_konglomerasi, sector, is_fundamental_solid, is_blue_chip, \
                     is_plan_to_trade, catatan, catatan_owner, foto_owner, net_income, takeprofit_wyckoff, \
                     wyckoff_phase_element, wyckoff_trading_range, wyckoff_horizontal_line, catatan_pribadi";
                let q = format!(
                    "SELECT {COLUMNS} FROM {} \
                     WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?",
                    self.table
                );
                let mut scan = self.session.prepare(q).await?;
                scan.set_page_size(PAGE_SIZE);

                let q = format!("SELECT {COLUMNS} FROM {} WHERE emiten_name = ?", self.table);
                let by_emiten_name = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_fundamental_solid = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_fundamental_solid = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET sector = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_sector = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_konglomerasi = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_konglomerasi = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_blue_chip = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_blue_chip = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET is_plan_to_trade = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_plan_to_trade = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_catatan = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan_owner = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_catatan_owner = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET catatan_pribadi = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_catatan_pribadi = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET foto_owner = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_foto_owner = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET takeprofit_wyckoff = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_takeprofit_wyckoff = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET wyckoff_phase_element = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_wyckoff_phase_element = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET wyckoff_trading_range = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_wyckoff_trading_range = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET wyckoff_horizontal_line = ? WHERE emiten_name = ?",
                    self.table
                );
                let update_wyckoff_horizontal_line = self.session.prepare(q).await?;

                let q = format!(
                    "SELECT agg_tahun_bulan_tanggal_emiten_name FROM {} WHERE emiten_name = ?",
                    self.trending_mv
                );
                let trending_aggs_by_emiten = self.session.prepare(q).await?;

                let q = format!(
                    "UPDATE {} SET sector = ? WHERE agg_tahun_bulan_tanggal_emiten_name = ?",
                    self.trending_table
                );
                let update_trending_sector = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    scan,
                    by_emiten_name,
                    update_fundamental_solid,
                    update_sector,
                    update_konglomerasi,
                    update_blue_chip,
                    update_plan_to_trade,
                    update_catatan,
                    update_catatan_owner,
                    update_catatan_pribadi,
                    update_foto_owner,
                    update_takeprofit_wyckoff,
                    update_wyckoff_phase_element,
                    update_wyckoff_trading_range,
                    update_wyckoff_horizontal_line,
                    trending_aggs_by_emiten,
                    update_trending_sector,
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

    /// Lookup satu baris via PK base table: `WHERE emiten_name = ?`.
    pub async fn get_by_emiten_name(
        &self,
        emiten_name: &str,
    ) -> Result<Option<EmitenList>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.by_emiten_name, (emiten_name,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<EmitenList>()?)
    }

    /// Lookup banyak emiten via PK. Yang tidak ada di-skip.
    pub async fn get_many_by_emiten_names(
        &self,
        emiten_names: &[String],
    ) -> Result<Vec<EmitenList>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::with_capacity(emiten_names.len());
        for name in emiten_names {
            if let Some(row) = self.get_by_emiten_name(name).await? {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// Update `is_fundamental_solid`. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_fundamental_solid(
        &self,
        emiten_name: &str,
        is_fundamental_solid: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_fundamental_solid,
                (is_fundamental_solid, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `emiten_list.sector` + sync tinyint yang sama ke semua `emiten_trending.sector`
    /// untuk emiten yang sama. Mengembalikan `Ok(None)` bila `emiten_name` tidak ada;
    /// `Ok(Some(n))` = berhasil, `n` = jumlah baris trending yang di-update.
    pub async fn update_sector(
        &self,
        emiten_name: &str,
        sector: i8,
    ) -> Result<Option<usize>, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(None);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_sector, (sector, emiten_name))
            .await?;

        #[derive(scylla::DeserializeRow)]
        struct TrendingAggRow {
            #[scylla(default_when_null)]
            agg_tahun_bulan_tanggal_emiten_name: String,
        }

        let mv = self
            .session
            .execute_unpaged(&prepared.trending_aggs_by_emiten, (emiten_name,))
            .await?
            .into_rows_result()?;

        let mut n = 0usize;
        for row in mv.rows::<TrendingAggRow>()? {
            let agg = row?.agg_tahun_bulan_tanggal_emiten_name;
            if agg.is_empty() {
                continue;
            }
            self.session
                .execute_unpaged(
                    &prepared.update_trending_sector,
                    (sector, agg.as_str()),
                )
                .await?;
            n += 1;
        }
        Ok(Some(n))
    }

    /// Update `is_konglomerasi`. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_konglomerasi(
        &self,
        emiten_name: &str,
        is_konglomerasi: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_konglomerasi,
                (is_konglomerasi, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `is_blue_chip`. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_blue_chip(
        &self,
        emiten_name: &str,
        is_blue_chip: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_blue_chip, (is_blue_chip, emiten_name))
            .await?;
        Ok(true)
    }

    /// Update `is_plan_to_trade`. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_plan_to_trade(
        &self,
        emiten_name: &str,
        is_plan_to_trade: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_plan_to_trade,
                (is_plan_to_trade, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `catatan` (map). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_catatan(
        &self,
        emiten_name: &str,
        catatan: &HashMap<String, String>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_catatan, (catatan, emiten_name))
            .await?;
        Ok(true)
    }

    /// Update `catatan_owner`. Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_catatan_owner(
        &self,
        emiten_name: &str,
        catatan_owner: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_catatan_owner,
                (catatan_owner, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `catatan_pribadi` (text). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_catatan_pribadi(
        &self,
        emiten_name: &str,
        catatan_pribadi: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_catatan_pribadi,
                (catatan_pribadi, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `foto_owner` (list<path>). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_foto_owner(
        &self,
        emiten_name: &str,
        foto_owner: &[String],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.update_foto_owner, (foto_owner, emiten_name))
            .await?;
        Ok(true)
    }

    /// Update `takeprofit_wyckoff` (map). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_takeprofit_wyckoff(
        &self,
        emiten_name: &str,
        takeprofit_wyckoff: &HashMap<String, String>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_takeprofit_wyckoff,
                (takeprofit_wyckoff, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `wyckoff_phase_element` (map). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_wyckoff_phase_element(
        &self,
        emiten_name: &str,
        wyckoff_phase_element: &HashMap<String, Vec<String>>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_wyckoff_phase_element,
                (wyckoff_phase_element, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `wyckoff_trading_range` (list). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_wyckoff_trading_range(
        &self,
        emiten_name: &str,
        wyckoff_trading_range: &[i32],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_wyckoff_trading_range,
                (wyckoff_trading_range, emiten_name),
            )
            .await?;
        Ok(true)
    }

    /// Update `wyckoff_horizontal_line` (list). Mengembalikan `Ok(false)` bila `emiten_name` tidak ada.
    pub async fn update_wyckoff_horizontal_line(
        &self,
        emiten_name: &str,
        wyckoff_horizontal_line: &[i32],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_emiten_name(emiten_name).await?.is_none() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(
                &prepared.update_wyckoff_horizontal_line,
                (wyckoff_horizontal_line, emiten_name),
            )
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
