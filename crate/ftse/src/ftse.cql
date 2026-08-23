-- Keyspace: invezgood
-- Rating FTSE per emiten (PK code). Skema kolom sama persis dengan invezgood.msci.
-- Tabel dibuat via MCP Scylla (2026-08-23).

CREATE TABLE IF NOT EXISTS invezgood.ftse (
    code       text PRIMARY KEY,
    grade      text,
    status     text,
    updated_at timestamp
);

-- Skema:
-- | column_name | kind          | type      |
-- |-------------|---------------|-----------|
-- | code        | partition_key | text      |
-- | grade       | regular       | text      |
-- | status      | regular       | text      |
-- | updated_at  | regular       | timestamp |
