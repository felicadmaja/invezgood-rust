-- Keyspace: invezgood
-- Holdings portofolio (mirror stockbit_ws).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.portofolio (
    emiten_name   text PRIMARY KEY,
    long_name     text,
    emiten_icon   text,
    balance_lot   bigint,
    available_lot bigint,
    average_price double,
    current_price double,
    invested      double,
    market_value  double,
    potential_p_l double,
    percentage    double
);

CREATE TABLE IF NOT EXISTS invezgood.portofolio_equity (
    nama  text PRIMARY KEY,
    value double
);

-- Skema aktual portofolio:
-- | column_name   | kind          | type   |
-- |---------------|---------------|--------|
-- | emiten_name   | partition_key | text   |
-- | long_name     | regular       | text   |
-- | emiten_icon   | regular       | text   |
-- | balance_lot   | regular       | bigint |
-- | available_lot | regular       | bigint |
-- | average_price | regular       | double |
-- | current_price | regular       | double |
-- | invested      | regular       | double |
-- | market_value  | regular       | double |
-- | potential_p_l | regular       | double |
-- | percentage    | regular       | double |
--
-- portofolio_equity: PK nama (Trading Balance, Invested, Open, Net Profit Loss, Total Equity)
