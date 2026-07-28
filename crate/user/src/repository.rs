use std::sync::Arc;

use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::database::keyspace;
use crate::model::{UserIdByEmail, UserPublicRow, UserRow};

struct Prepared {
    by_email_mv: PreparedStatement,
    by_id: PreparedStatement,
    update_password: PreparedStatement,
    insert_user: PreparedStatement,
    delete_user: PreparedStatement,
    get_all: PreparedStatement,
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
                let update_password = self
                    .session
                    .prepare(format!(
                        "UPDATE {}.user SET password = ? WHERE id = ?",
                        self.keyspace
                    ))
                    .await?;
                let insert_user = self
                    .session
                    .prepare(format!(
                        "INSERT INTO {}.user (id, name, email, password) VALUES (?, ?, ?, ?)",
                        self.keyspace
                    ))
                    .await?;
                let delete_user = self
                    .session
                    .prepare(format!(
                        "DELETE FROM {}.user WHERE id = ?",
                        self.keyspace
                    ))
                    .await?;
                let get_all = self
                    .session
                    .prepare(format!(
                        "SELECT id, name, email FROM {}.user",
                        self.keyspace
                    ))
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Prepared {
                    by_email_mv,
                    by_id,
                    update_password,
                    insert_user,
                    delete_user,
                    get_all,
                })
            })
            .await
    }

    /// Preflight cache prepared statements — wajib di-await di binary utama sebelum serve.
    pub async fn warm_prepared_statements(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.prepared().await?;
        Ok(())
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

    /// Update kolom `password` (bcrypt hash) di tabel `user`.
    pub async fn update_password(
        &self,
        id: Uuid,
        password_hash: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        self.session
            .execute_unpaged(&p.update_password, (password_hash, id))
            .await?;
        Ok(())
    }

    /// Insert user baru (MV `user_by_email` ikut terisi dari base table).
    pub async fn insert_user(
        &self,
        id: Uuid,
        name: &str,
        email: &str,
        password_hash: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        self.session
            .execute_unpaged(&p.insert_user, (id, name, email, password_hash))
            .await?;
        Ok(())
    }

    /// Hapus user by id (MV `user_by_email` ikut ter-update).
    pub async fn delete_user(
        &self,
        id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        self.session
            .execute_unpaged(&p.delete_user, (id,))
            .await?;
        Ok(())
    }

    /// Semua user (id, name, email) — tanpa password.
    pub async fn get_all(
        &self,
    ) -> Result<Vec<UserPublicRow>, Box<dyn std::error::Error + Send + Sync>> {
        let p = self.prepared().await?;
        let result = self
            .session
            .execute_unpaged(&p.get_all, &[])
            .await?
            .into_rows_result()?;
        let mut rows = Vec::new();
        for row in result.rows::<UserPublicRow>()? {
            rows.push(row?);
        }
        Ok(rows)
    }
}
