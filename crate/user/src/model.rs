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
}

#[derive(Debug, Clone, scylla::DeserializeRow)]
pub struct UserIdByEmail {
    pub id: Uuid,
}
