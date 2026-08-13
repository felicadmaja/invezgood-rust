-- Keyspace: invezgood
-- Rating MSCI per emiten (PK code).

CREATE TABLE IF NOT EXISTS invezgood.msci (
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
