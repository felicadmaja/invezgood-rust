use uuid::Uuid;

#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct UserRow {
    pub id: Uuid,
    #[scylla(default_when_null)]
    pub name: String,
    #[scylla(default_when_null)]
    pub email: String,
    #[scylla(default_when_null)]
    pub password: String,
    #[scylla(default_when_null)]
    pub role: String,
}

/// Baris publik untuk list user (tanpa password).
#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct UserPublicRow {
    pub id: Uuid,
    #[scylla(default_when_null)]
    pub name: String,
    #[scylla(default_when_null)]
    pub email: String,
    #[scylla(default_when_null)]
    pub role: String,
}

#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct UserIdByEmail {
    pub id: Uuid,
}
