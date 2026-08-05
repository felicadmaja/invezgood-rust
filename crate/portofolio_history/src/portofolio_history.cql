-- Keyspace: invezgood
-- Riwayat transaksi portfolio per emiten (dari Stockbit /history).

CREATE TYPE IF NOT EXISTS invezgood.portofolio_history_item (
    command text,
    symbol  text,
    price   double,
    lot     double,
    amount  double,
    status  text
);

CREATE TABLE IF NOT EXISTS invezgood.portofolio_history (
    emiten_name         text,
    tahun_bulan_tanggal date,
    tahun_bulan         text,
    history             list<frozen<portofolio_history_item>>,
    PRIMARY KEY ((emiten_name), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.portofolio_history_by_tahun_bulan AS
    SELECT * FROM invezgood.portofolio_history
    WHERE tahun_bulan IS NOT NULL
      AND emiten_name IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (tahun_bulan, emiten_name, tahun_bulan_tanggal)
    WITH CLUSTERING ORDER BY (emiten_name ASC, tahun_bulan_tanggal DESC);

-- Skema:
-- | column_name         | kind            | type                                      |
-- |---------------------|-----------------|-------------------------------------------|
-- | emiten_name         | partition_key   | text                                      |
-- | tahun_bulan_tanggal | clustering DESC | date                                      |
-- | tahun_bulan         | regular         | text (YYYY-MM dari tahun_bulan_tanggal)   |
-- | history             | regular         | list<frozen<portofolio_history_item>>     |
--
-- MV portofolio_history_by_tahun_bulan:
-- PK (tahun_bulan, emiten_name, tahun_bulan_tanggal DESC)
-- Query: WHERE tahun_bulan = 'YYYY-MM'
--
-- Migrasi (jalan sekali bila tabel sudah ada tanpa tahun_bulan):
-- ALTER TABLE invezgood.portofolio_history ADD tahun_bulan text;
-- UPDATE ... SET tahun_bulan = 'YYYY-MM' WHERE emiten_name = ? AND tahun_bulan_tanggal = ?;
-- lalu CREATE MATERIALIZED VIEW di atas.
