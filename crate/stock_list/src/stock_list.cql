-- Keyspace: invezgood
-- Tabel dibuat via MCP Scylla (2026-08-02)

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.stock_list (
    code   text PRIMARY KEY,
    name   text,
    sector text,
    logo   text
);

-- Skema aktual (system_schema.columns):
-- | column_name | kind           | type |
-- |-------------|----------------|------|
-- | code        | partition_key  | text |
-- | name        | regular        | text |
-- | sector      | regular        | text |
-- | logo        | regular        | text |
