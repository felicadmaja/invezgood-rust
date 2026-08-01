use std::sync::Arc;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

pub async fn connect() -> Result<Arc<Session>, Box<dyn std::error::Error + Send + Sync>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".into());
    let user = std::env::var("SCYLLA_USER").unwrap_or_else(|_| "cassandra".into());
    let password = std::env::var("SCYLLA_PASSWORD").unwrap_or_default();

    let session = SessionBuilder::new()
        .known_node(uri)
        .user(user, password)
        .build()
        .await?;

    Ok(Arc::new(session))
}
