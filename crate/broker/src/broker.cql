-- Keyspace: invezgood
-- Daftar broker/sekuritas BEI.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.broker (
    broker_code text PRIMARY KEY,
    name        text,
    tipe        tinyint,
    asosiasi    text,
    catatan     text,
    is_huge     boolean,
    updated_at  timestamp
);

-- Skema aktual:
-- | column_name | kind          | type      |
-- |-------------|---------------|-----------|
-- | broker_code | partition_key | text      |
-- | name        | regular       | text      |
-- | tipe        | regular       | tinyint (0=local, 1=foreign, 2=government; default 0) |
-- | asosiasi    | regular       | text      |
-- | catatan     | regular       | text      |
-- | is_huge     | regular       | boolean   |
-- | updated_at  | regular       | timestamp |
--
-- Contoh baris (API list/broker):
-- { "code": "CC", "name": "MANDIRI SEKURITAS" }
--
-- Migrasi tipe text → tinyint (jalan sekali bila tabel sudah ada):
-- ALTER TABLE invezgood.broker DROP tipe;
-- ALTER TABLE invezgood.broker ADD tipe tinyint;
-- Backfill default 0 (per PK; Scylla tidak mendukung UPDATE tanpa WHERE):
-- UPDATE invezgood.broker SET tipe = 0 WHERE broker_code = '<code>';
--
-- Migrasi is_huge (jalan sekali bila tabel sudah ada tanpa kolom):
-- ALTER TABLE invezgood.broker ADD is_huge boolean;
