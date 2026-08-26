-- Keyspace: invezgood
-- Tabel dibuat via MCP Scylla (2026-08-27)

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.top_foreign_flow (
    tahun_bulan_tanggal date,
    code                text,
    name                text,
    price               int,
    change              double,
    value               bigint,
    volume              bigint,
    accum_or_dist       text,
    PRIMARY KEY (tahun_bulan_tanggal, value, code)
) WITH CLUSTERING ORDER BY (value DESC, code ASC);

-- Skema aktual (system_schema.columns):
-- | column_name         | kind            | type   |
-- |---------------------|-----------------|--------|
-- | tahun_bulan_tanggal | partition_key   | date   |
-- | value               | clustering_key  | bigint |
-- | code                | clustering_key  | text   |
-- | name                | regular         | text   |
-- | price               | regular         | int    |
-- | change              | regular         | double |
-- | volume              | regular         | bigint |
-- | accum_or_dist       | regular         | text   |
--
-- value/volume bigint: nilai dari Invezgo bisa > i32::MAX.
-- Re-fetch: DELETE partition tanggal lalu INSERT ulang (value ada di clustering key).
