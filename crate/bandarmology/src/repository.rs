use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::{agg_tahun_bulan_emiten_name, Bandarmology, BandarmologyHarian};

struct Prepared {
    by_agg: PreparedStatement,
    harian_by_pk: PreparedStatement,
}

pub struct BandarmologyRepository {
    session: Arc<Session>,
    table: String,
    table_harian: String,
    prepared: OnceCell<Prepared>,
}

impl BandarmologyRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.bandarmology"),
            table_harian: format!("{ks}.bandarmology_harian"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT agg_tahun_bulan_emiten_name, emiten_name, tahun_bulan, \
                     broker_summary_current_w1, broker_summary_current_w2, \
                     broker_summary_current_w3, broker_summary_current_w4, \
                     broker_summary, updated_at \
                     FROM {} WHERE agg_tahun_bulan_emiten_name = ? LIMIT 1",
                    self.table
                );
                let harian_q = format!(
                    "SELECT emiten_name, tahun_bulan_tanggal, broker_summary_harian, updated_at \
                     FROM {} WHERE emiten_name = ? AND tahun_bulan_tanggal = ? LIMIT 1",
                    self.table_harian
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    by_agg: self.session.prepare(q).await?,
                    harian_by_pk: self.session.prepare(harian_q).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn find_by_tahun_bulan_and_emiten(
        &self,
        tahun_bulan: &str,
        kode_emiten: &str,
    ) -> Result<Option<Bandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let agg = agg_tahun_bulan_emiten_name(tahun_bulan, kode_emiten);
        let result = self
            .session
            .execute_unpaged(&prepared.by_agg, (agg.as_str(),))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<Bandarmology>()?)
    }

    /// Lookup banyak emiten untuk satu `tahun_bulan` (PK per kode). Yang tidak ada di-skip.
    pub async fn find_many_by_tahun_bulan_and_emitens(
        &self,
        tahun_bulan: &str,
        kode_emitens: &[String],
    ) -> Result<Vec<Bandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::with_capacity(kode_emitens.len());
        for kode in kode_emitens {
            if let Some(row) = self
                .find_by_tahun_bulan_and_emiten(tahun_bulan, kode)
                .await?
            {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// Lookup banyak `tahun_bulan` untuk satu emiten. Yang tidak ada di-skip.
    /// Returns pasangan `(tahun_bulan, row)`.
    pub async fn find_many_by_emiten_and_tahun_bulans(
        &self,
        emiten_name: &str,
        tahun_bulans: &[String],
    ) -> Result<Vec<(String, Bandarmology)>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::with_capacity(tahun_bulans.len());
        for tb in tahun_bulans {
            if let Some(row) = self.find_by_tahun_bulan_and_emiten(tb, emiten_name).await? {
                out.push((tb.clone(), row));
            }
        }
        Ok(out)
    }

    /// Lookup `bandarmology_harian` by PK `(emiten_name, tahun_bulan_tanggal)`.
    pub async fn find_harian_by_emiten_and_date(
        &self,
        emiten_name: &str,
        tahun_bulan_tanggal: NaiveDate,
    ) -> Result<Option<BandarmologyHarian>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(
                &prepared.harian_by_pk,
                (emiten_name, tahun_bulan_tanggal),
            )
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<BandarmologyHarian>()?)
    }
}
