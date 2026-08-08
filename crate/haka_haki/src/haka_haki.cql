-- Keyspace: invezgood
-- HAKA/HAKI per emiten per tanggal per jam:menit.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.haka_haki (
    code                text,
    tahun_bulan_tanggal date,
    jam_menit           text,
    volume              int,
    buy                 int,
    sell                int,
    PRIMARY KEY (code, tahun_bulan_tanggal, jam_menit)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, jam_menit DESC);

-- Skema aktual:
-- | column_name         | kind            | type                          |
-- |---------------------|-----------------|-------------------------------|
-- | code                | partition_key   | text (kode emiten)            |
-- | tahun_bulan_tanggal | clustering DESC | date (YYYY-MM-DD)             |
-- | jam_menit           | clustering DESC | text (HH:MM)                  |
-- | volume              | regular         | int                           |
-- | buy                 | regular         | int                           |
-- | sell                | regular         | int                           |
