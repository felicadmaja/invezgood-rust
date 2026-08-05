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
    history             list<frozen<portofolio_history_item>>,
    PRIMARY KEY ((emiten_name), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

-- Skema:
-- | column_name         | kind           | type                                      |
-- |---------------------|----------------|-------------------------------------------|
-- | emiten_name         | partition_key  | text                                      |
-- | tahun_bulan_tanggal | clustering DESC| date                                      |
-- | history             | regular        | list<frozen<portofolio_history_item>>     |
--
-- UDT portofolio_history_item: command, symbol, price, lot, amount, status
