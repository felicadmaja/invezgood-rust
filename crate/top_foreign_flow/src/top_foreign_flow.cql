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

-- MV: PK columns only — list emiten per tanggal (urut value DESC).
CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.top_foreign_flow_by_tahun_bulan_tanggal AS
    SELECT tahun_bulan_tanggal, value, code
    FROM invezgood.top_foreign_flow
    WHERE tahun_bulan_tanggal IS NOT NULL
      AND value IS NOT NULL
      AND code IS NOT NULL
    PRIMARY KEY (tahun_bulan_tanggal, value, code)
    WITH CLUSTERING ORDER BY (value DESC, code ASC);

-- MV: PK columns only — riwayat foreign flow per emiten (tanggal DESC).
CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.top_foreign_flow_by_code AS
    SELECT code, tahun_bulan_tanggal, value
    FROM invezgood.top_foreign_flow
    WHERE code IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
      AND value IS NOT NULL
    PRIMARY KEY (code, tahun_bulan_tanggal, value)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, value DESC);

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
-- MV top_foreign_flow_by_tahun_bulan_tanggal:
-- PK (tahun_bulan_tanggal, value DESC, code ASC) — SELECT PK only
-- Query: WHERE tahun_bulan_tanggal = 'YYYY-MM-DD'
--
-- MV top_foreign_flow_by_code:
-- PK (code, tahun_bulan_tanggal DESC, value DESC) — SELECT PK only
-- Query: WHERE code = 'BBCA'
--
-- value/volume bigint: nilai dari Invezgo bisa > i32::MAX.
-- Re-fetch: DELETE partition tanggal lalu INSERT ulang (value ada di clustering key).
