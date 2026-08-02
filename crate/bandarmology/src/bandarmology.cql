-- Keyspace: invezgood
-- Tabel bandarmology per emiten per tanggal — list ringkasan transaksi per broker.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TYPE IF NOT EXISTS invezgood.bandarmology_entry (
    code         text,
    buy_freq     text,
    buy_volume   text,
    buy_value    text,
    sell_freq    text,
    sell_volume  text,
    sell_value   text,
    buy_avg      double,
    sell_avg     double,
    net_value    text,
    net_volume   text,
    net_freq     text,
    name         text
);

CREATE TABLE IF NOT EXISTS invezgood.bandarmology (
    code                text,
    tahun_bulan_tanggal date,
    bandarmology        frozen<list<frozen<bandarmology_entry>>>,
    updated_at          timestamp,
    PRIMARY KEY (code, tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

-- Skema aktual:
-- | column_name         | kind            | type                                              |
-- |---------------------|-----------------|---------------------------------------------------|
-- | code                | partition_key   | text (kode emiten, mis. BBCA)                     |
-- | tahun_bulan_tanggal | clustering_key  | date (DESC)                                       |
-- | bandarmology        | regular         | frozen<list<frozen<bandarmology_entry>>>          |
-- | updated_at          | regular         | timestamp                                         |
--
-- UDT bandarmology_entry.code = kode broker (mis. CC, XL), bukan kode emiten.
--
-- Contoh elemen bandarmology (broker CC pada emiten BBCA):
-- {
--   "code": "CC",
--   "buy_freq": "17490",
--   "buy_volume": "141218100",
--   "buy_value": "52671124000",
--   "sell_freq": "43890",
--   "sell_volume": "259388900",
--   "sell_value": "96828312800",
--   "buy_avg": 372.9771466972009962,
--   "sell_avg": 373.2939721013505204,
--   "net_value": "-44157188800",
--   "net_volume": "-118170800",
--   "net_freq": "-26400",
--   "name": "MANDIRI SEKURITAS"
-- }
