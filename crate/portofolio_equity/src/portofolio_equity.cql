-- Keyspace: invezgood
-- Ringkasan equity portofolio (Trading Balance, Invested, Open, Net P/L, Total Equity).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.portofolio_equity (
    nama  text PRIMARY KEY,
    value double
);

-- | column_name | kind          | type   |
-- |-------------|---------------|--------|
-- | nama        | partition_key | text   |
-- | value       | regular       | double |
