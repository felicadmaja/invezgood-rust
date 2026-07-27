use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::BandarmologyHarian;

struct Prepared {
    harian_by_pk: PreparedStatement,
}

pub struct BandarmologyRepository {
    session: Arc<Session>,
    table_harian: String,
    prepared: OnceCell<Prepared>,
}

impl BandarmologyRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table_harian: format!("{ks}.bandarmology_harian"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let harian_q = format!(
                    "SELECT emiten_name, tahun_bulan_tanggal, broker_summary_harian, updated_at \
                     FROM {} WHERE emiten_name = ? AND tahun_bulan_tanggal = ? LIMIT 1",
                    self.table_harian
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    harian_by_pk: self.session.prepare(harian_q).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
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

    /// Lookup banyak tanggal untuk satu emiten. Yang tidak ada di-skip.
    pub async fn find_many_harian_by_emiten_and_dates(
        &self,
        emiten_name: &str,
        dates: &[NaiveDate],
    ) -> Result<Vec<BandarmologyHarian>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::with_capacity(dates.len());
        for day in dates {
            if let Some(row) = self
                .find_harian_by_emiten_and_date(emiten_name, *day)
                .await?
            {
                out.push(row);
            }
        }
        Ok(out)
    }
}
