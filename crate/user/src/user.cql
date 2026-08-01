-- Keyspace: invezgood
-- Tabel dibuat via MCP Scylla (2026-08-02)

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.user (
    email    text PRIMARY KEY,
    nama     text,
    password text,
    role     text
);

-- Skema aktual (system_schema.columns):
-- | column_name | kind           | type |
-- |-------------|----------------|------|
-- | email       | partition_key  | text |
-- | nama        | regular        | text |
-- | password    | regular        | text |
-- | role        | regular        | text |
