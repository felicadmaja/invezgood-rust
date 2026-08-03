-- Keyspace: invezgood
-- Tabel + MV dibuat via MCP Scylla (2026-08-03)

-- Table: invezgood.emiten_trending
-- PK: ((agg_tahun_bulan_tanggal_emiten_name)) text — contoh "2026-08-03_BBCA"
-- | column_name                         | kind          | type      |
-- |-------------------------------------|---------------|-----------|
-- | agg_tahun_bulan_tanggal_emiten_name | partition_key | text      |
-- | tahun_bulan_tanggal                 | regular       | date      |
-- | gainer_or_loser                     | regular       | text      |
-- | emiten_name                         | regular       | text      |
-- | long_name                           | regular       | text      |
-- | emiten_icon                         | regular       | text      |
-- | sector                              | regular       | tinyint   |
-- | price                               | regular       | double    |
-- | price_change                        | regular       | double    |
-- | value                               | regular       | text      |
-- | volume                              | regular       | text      |
-- | freq                                | regular       | text      |
-- | updated_at                          | regular       | timestamp |

-- MV: invezgood.emiten_trending_by_tahun_bulan_tanggal
-- PK: (tahun_bulan_tanggal), agg_tahun_bulan_tanggal_emiten_name

-- MV: invezgood.emiten_trending_by_emiten_name
-- PK: (emiten_name), agg_tahun_bulan_tanggal_emiten_name
