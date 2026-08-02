-- Keyspace: invezgood
-- Daftar broker/sekuritas BEI.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.broker (
    broker_code text PRIMARY KEY,
    name        text,
    tipe        text,
    asosiasi    text,
    catatan     text,
    updated_at  timestamp
);

-- Skema aktual:
-- | column_name | kind          | type      |
-- |-------------|---------------|-----------|
-- | broker_code | partition_key | text      |
-- | name        | regular       | text      |
-- | tipe        | regular       | text      |
-- | asosiasi    | regular       | text      |
-- | catatan     | regular       | text      |
-- | updated_at  | regular       | timestamp |
--
-- Contoh baris (API list/broker):
-- { "code": "CC", "name": "MANDIRI SEKURITAS" }
