use std::sync::Arc;

use futures_util::StreamExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::PortofolioBandarmology;

const PAGE_SIZE: i32 = 100;

struct Prepared {
    by_emiten: PreparedStatement,
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
                let mut by_emiten = self
                    .session
                    .prepare(format!(
                        "SELECT emiten_name, tahun_bulan_tanggal, bandarmology \
                         FROM {} WHERE emiten_name = ?",
                        self.table
                    ))
                    .await?;
                by_emiten.set_page_size(PAGE_SIZE);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { by_emiten })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    /// Semua baris untuk satu `emiten_name` (urutan clustering DESC).
    pub async fn find_by_emiten(
        &self,
        emiten_name: &str,
    ) -> Result<Vec<PortofolioBandarmology>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let pager = self
            .session
            .execute_iter(prepared.by_emiten.clone(), (emiten_name,))
            .await?;
        let mut stream = pager.rows_stream::<PortofolioBandarmology>()?;
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row?);
        }
        Ok(out)
    }
}
