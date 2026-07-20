use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::sync::Arc;

pub async fn session() -> Result<Arc<Session>, Box<dyn std::error::Error + Send + Sync>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri.as_str());
    if let Ok(user) = std::env::var("SCYLLA_USER") {
        if let Ok(password) = std::env::var("SCYLLA_PASSWORD") {
            builder = builder.user(user, password);
        }
    }
    Ok(Arc::new(builder.build().await?))
}

pub fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}
