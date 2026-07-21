use std::sync::Arc;

use chrono::NaiveDate;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PortofolioBandarmology;

struct Prepared {
    by_pk: PreparedStatement,
}

pub struct PortofolioBandarmologyRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl PortofolioBandarmologyRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.portofolio_bandarmology"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT emiten_name, tahun_bulan_tanggal, bandarmology \
                     FROM {} WHERE emiten_name = ? AND tahun_bulan_tanggal = ? LIMIT 1",
                    self.table
                );
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    by_pk: self.session.prepare(q).await?,
                })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn find_by_emiten_and_date(
        &self,
        emiten_name: &str,
        tahun_bulan_tanggal: NaiveDate,
    ) -> Result<Option<PortofolioBandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(
                &prepared.by_pk,
                (emiten_name, tahun_bulan_tanggal),
            )
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<PortofolioBandarmology>()?)
    }
}
