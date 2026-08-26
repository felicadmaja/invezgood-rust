use std::sync::Arc;

use chrono::NaiveDate;
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{EmitenTrending, KEYSPACE, MV_BY_DATE};

const MV_BY_DATE_Q: &str = "SELECT agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, \
    gainer_or_loser, emiten_name, long_name, emiten_icon, sector, price, \
    price_change, value, volume, freq, updated_at \
    FROM invezgood.emiten_trending_by_tahun_bulan_tanggal WHERE tahun_bulan_tanggal = ?";

const UPSERT: &str = "INSERT INTO invezgood.emiten_trending \
    (agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, gainer_or_loser, emiten_name, \
    long_name, emiten_icon, sector, price, price_change, value, volume, freq, updated_at) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const SECTOR_BY_CODE: &str = "SELECT sector FROM invezgood.stock_list WHERE code = ?";

#[derive(Debug, scylla::DeserializeRow)]
struct SectorRow {
    #[scylla(default_when_null)]
    sector: Option<String>,
}

pub struct EmitenTrendingRepository {
    session: Arc<Session>,
}

impl EmitenTrendingRepository {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    pub async fn get_all_by_date(&self, date: NaiveDate) -> Result<Vec<EmitenTrending>, String> {
        let mut rows = self
            .session
            .query_iter(MV_BY_DATE_Q, (date,))
            .await
            .map_err(|e| format!("MV {KEYSPACE}.{MV_BY_DATE} date={date}: {e}"))?
            .rows_stream::<EmitenTrending>()
            .map_err(|e| format!("MV stream {KEYSPACE}.{MV_BY_DATE}: {e}"))?;

        let mut out = Vec::new();
        while let Some(item) = rows
            .try_next()
            .await
            .map_err(|e| format!("MV row {KEYSPACE}.{MV_BY_DATE}: {e}"))?
        {
            out.push(item);
        }

        Ok(out)
    }

    pub async fn upsert(&self, row: &EmitenTrending) -> Result<(), String> {
        self.session
            .query_unpaged(
                UPSERT,
                (
                    row.agg_tahun_bulan_tanggal_emiten_name.as_str(),
                    row.tahun_bulan_tanggal,
                    row.gainer_or_loser.as_str(),
                    row.emiten_name.as_str(),
                    row.long_name.as_str(),
                    row.emiten_icon.as_str(),
                    row.sector,
                    row.price,
                    row.price_change,
                    row.value.as_str(),
                    row.volume.as_str(),
                    row.freq.as_str(),
                    row.updated_at,
                ),
            )
            .await
            .map_err(|e| {
                format!(
                    "upsert {KEYSPACE}.emiten_trending agg={}: {e}",
                    row.agg_tahun_bulan_tanggal_emiten_name
                )
            })?;
        Ok(())
    }

    /// Sector dari `invezgood.stock_list` — kolom DB `tinyint`, jadi hanya terisi bila teks sector numerik.
    pub async fn lookup_sector_i8(&self, code: &str) -> Result<Option<i8>, String> {
        let mut rows = self
            .session
            .query_iter(SECTOR_BY_CODE, (code,))
            .await
            .map_err(|e| format!("sector lookup stock_list code={code}: {e}"))?
            .rows_stream::<SectorRow>()
            .map_err(|e| format!("sector stream stock_list code={code}: {e}"))?;

        let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| format!("sector row stock_list code={code}: {e}"))?
        else {
            return Ok(None);
        };

        let Some(raw) = row.sector else {
            return Ok(None);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        Ok(trimmed.parse::<i8>().ok())
    }
}
