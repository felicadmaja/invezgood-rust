-- Keyspace: invezgood
-- OHLCV harian per emiten (sumber: Invezgo analysis/chart/stock).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.chart (
    code   text PRIMARY KEY,
    date   date,
    open   int,
    high   int,
    low    int,
    close  int,
    volume int,
) WITH CLUSTERING ORDER BY (date DESC);

-- | column_name | kind            | type |
-- |-------------|-----------------|------|
-- | code        | partition_key   | text |
-- | date        | clustering_key  | date |
-- | open        | regular         | int  |
-- | high        | regular         | int  |
-- | low         | regular         | int  |
-- | close       | regular         | int  |
-- | volume      | regular         | int  |
--
-- Lookup: WHERE code = ? [AND date >= ? AND date <= ?]
