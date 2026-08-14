pub fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "invezgood".to_string())
}
