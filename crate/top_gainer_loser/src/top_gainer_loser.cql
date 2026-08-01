-- Keyspace: invezgood
-- Tabel dibuat via MCP Scylla (2026-08-02)

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TYPE IF NOT EXISTS invezgood.top_gainer_loser_graph_point (
    date  text,
    value double
);

CREATE TABLE IF NOT EXISTS invezgood.top_gainer_loser (
    tahun_bulan_tanggal date,
    code                text,
    name                text,
    price               double,
    change_pct          double,
    value               text,
    volume              text,
    logo                text,
    calculated_value    double,
    graph               frozen<list<frozen<top_gainer_loser_graph_point>>>,
    PRIMARY KEY (tahun_bulan_tanggal, code)
);

-- Skema aktual (system_schema.columns):
-- | column_name      | kind            | type                                              |
-- |------------------|-----------------|---------------------------------------------------|
-- | tahun_bulan_tanggal | partition_key   | date                                              |
-- | code             | clustering_key  | text                                              |
-- | name             | regular         | text                                              |
-- | price            | regular         | double                                            |
-- | change_pct       | regular         | double                                            |
-- | value            | regular         | text                                              |
-- | volume           | regular         | text                                              |
-- | logo             | regular         | text                                              |
-- | calculated_value | regular         | double                                            |
-- | graph            | regular         | frozen<list<frozen<top_gainer_loser_graph_point>>> |

-- Contoh baris (API top-change):
-- {
--   "code": "IATA",
--   "name": "Karya Pacific Energy",
--   "price": 76,
--   "change": 33.333,
--   "value": "116766853900",
--   "volume": "1640828700",
--   "logo": "https://storage.invezgo.com/icon/IATA.png",
--   "calculated_value": 33.333,
--   "graph": [{ "date": "2026-07-27", "value": 1.724 }, ...]
-- }
