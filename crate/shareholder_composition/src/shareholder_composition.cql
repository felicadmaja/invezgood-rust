-- Keyspace: invezgood
-- Komposisi pemegang saham per emiten per periode bulan.
-- Tabel dibuat via MCP Scylla (2026-08-27).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.shareholder_composition (
    code        text,
    tahun_bulan text,
    detail      map<text, text>,
    PRIMARY KEY (code, tahun_bulan)
) WITH CLUSTERING ORDER BY (tahun_bulan DESC);

-- MV: PK columns only — daftar periode komposisi per emiten (tanpa map detail).
CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.shareholder_composition_by_code AS
    SELECT code, tahun_bulan
    FROM invezgood.shareholder_composition
    WHERE code IS NOT NULL
      AND tahun_bulan IS NOT NULL
    PRIMARY KEY (code, tahun_bulan)
    WITH CLUSTERING ORDER BY (tahun_bulan DESC);

-- Skema aktual:
-- | column_name | kind              | type           |
-- |-------------|-------------------|----------------|
-- | code        | partition_key     | text           | kode emiten (mis. BBCA)
-- | tahun_bulan | clustering_key    | text           | periode YYYY-MM
-- | detail      | regular           | map<text,text> | pasangan key-value komposisi
--
-- MV shareholder_composition_by_code:
-- PK (code, tahun_bulan DESC) — SELECT PK only
-- Query: WHERE code = 'BBCA'
