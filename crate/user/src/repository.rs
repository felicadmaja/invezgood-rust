use std::sync::Arc;

use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::database::keyspace;
use crate::model::{UserIdByEmail, UserRow};

struct Prepared {
    by_email_mv: PreparedStatement,
    by_id: PreparedStatement,
}

pub struct UserRepository {
    session: Arc<Session>,
    keyspace: String,
    prepared: OnceCell<Prepared>,
}

impl UserRepository {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            keyspace: keyspace(),
            prepared: OnceCell::new(),
        }
    }

    async fn prepared(&self) -> Result<&Prepared, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .get_or_try_init(|| async {
                let by_email_mv = self
                    .session
                    .prepare(format!(
                        "SELECT id FROM {}.user_by_email WHERE email = ? LIMIT 1",
                        self.keyspace
                    ))
                    .await?;
                let by_id = self
                    .session
                    .prepare(format!(
                        "SELECT id, name, email, password FROM {}.user WHERE id = ? LIMIT 1",
                        self.keyspace
                    ))
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared { by_email_mv, by_id })
            })
            .await
    }

    pub async fn find_by_email(
        &self,
        email: &str,
    ) -> Result<Option<UserRow>, Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        let mv = self
            .session
            .execute_unpaged(&p.by_email_mv, (email,))
            .await?
            .into_rows_result()?;
        let Some(UserIdByEmail { id }) = mv.maybe_first_row::<UserIdByEmail>()? else {
            return Ok(None);
        };
        self.find_by_id(id).await
    }

    pub async fn find_by_id(
        &self,
        id: Uuid,
    ) -> Result<Option<UserRow>, Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&p.by_id, (id,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<UserRow>()?)
    }
}
