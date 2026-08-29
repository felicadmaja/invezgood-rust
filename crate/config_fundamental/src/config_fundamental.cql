-- Keyspace: invezgood
-- Konfigurasi threshold/param fundamental (PK key).
-- Tabel dibuat via MCP Scylla (2026-08-29).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.config_fundamental (
    key         text PRIMARY KEY,
    value       double,
    description text
);

-- Skema aktual:
-- | column_name | kind          | type   |
-- |-------------|---------------|--------|
-- | key         | partition_key | text   |
-- | value       | regular       | double |
-- | description | regular       | text   |
