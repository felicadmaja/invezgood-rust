use std::sync::Arc;

use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::database::keyspace;
use crate::model::WyckoffGlossary;

struct Prepared {
    by_name: PreparedStatement,
    insert: PreparedStatement,
}

pub struct WyckoffGlossaryRepository {
    session: Arc<Session>,
    table: String,
    prepared: OnceCell<Prepared>,
}

impl WyckoffGlossaryRepository {
    pub fn new(session: Arc<Session>) -> Self {
        let ks = keyspace();
        Self {
            session,
            table: format!("{ks}.wyckoff_glossary"),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let q = format!(
                    "SELECT name, description FROM {} WHERE name = ?",
                    self.table
                );
                let by_name = self.session.prepare(q).await?;

                let q = format!(
                    "INSERT INTO {} (name, description) VALUES (?, ?)",
                    self.table
                );
                let insert = self.session.prepare(q).await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { by_name, insert })
            })
            .await
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
    }

    pub async fn get_by_name(
        &self,
        name: &str,
    ) -> Result<Option<WyckoffGlossary>, Box<dyn std::error::Error + Send + Sync>> {
        let prepared = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&prepared.by_name, (name,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<WyckoffGlossary>()?)
    }

    /// Insert entri baru. Mengembalikan `Ok(false)` bila `name` sudah ada.
    pub async fn insert(
        &self,
        name: &str,
        description: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.get_by_name(name).await?.is_some() {
            return Ok(false);
        }

        let prepared = self.prepared().await?;
        self.session
            .execute_unpaged(&prepared.insert, (name, description))
            .await?;
        Ok(true)
    }
}
