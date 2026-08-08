-- Keyspace: invezgood
-- HAKA/HAKI per emiten per tanggal per jam:menit.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.haka_haki (
    code                         text,
    tahun_bulan_tanggal          date,
    jam_menit                    text,
    agg_code_tahun_bulan_tanggal text,
    volume                       int,
    buy                          int,
    sell                         int,
    PRIMARY KEY (code, tahun_bulan_tanggal, jam_menit)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, jam_menit DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.haka_haki_by_agg_code_tahun_bulan_tanggal AS
    SELECT * FROM invezgood.haka_haki
    WHERE agg_code_tahun_bulan_tanggal IS NOT NULL
      AND code IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
      AND jam_menit IS NOT NULL
    PRIMARY KEY (agg_code_tahun_bulan_tanggal, code, tahun_bulan_tanggal, jam_menit)
    WITH CLUSTERING ORDER BY (code ASC, tahun_bulan_tanggal DESC, jam_menit DESC);

-- Skema aktual:
-- | column_name                  | kind            | type                          |
-- |------------------------------|-----------------|-------------------------------|
-- | code                         | partition_key   | text (kode emiten)            |
-- | tahun_bulan_tanggal          | clustering DESC | date (YYYY-MM-DD)             |
-- | jam_menit                    | clustering DESC | text (HH:MM)                  |
-- | agg_code_tahun_bulan_tanggal | regular         | text (code_YYYY-MM-DD)        |
-- | volume                       | regular         | int                           |
-- | buy                          | regular         | int                           |
-- | sell                         | regular         | int                           |
--
-- MV haka_haki_by_agg_code_tahun_bulan_tanggal:
-- PK (agg_code_tahun_bulan_tanggal, code, tahun_bulan_tanggal DESC, jam_menit DESC)
-- Query: WHERE agg_code_tahun_bulan_tanggal = 'BBCA_2026-08-07'
--
-- Migrasi (jalan sekali bila tabel sudah ada tanpa agg_code_tahun_bulan_tanggal):
-- ALTER TABLE invezgood.haka_haki ADD agg_code_tahun_bulan_tanggal text;
-- UPDATE ... SET agg_code_tahun_bulan_tanggal = 'CODE_YYYY-MM-DD' WHERE code = ? AND tahun_bulan_tanggal = ? AND jam_menit = ?;
-- lalu CREATE MATERIALIZED VIEW di atas.
