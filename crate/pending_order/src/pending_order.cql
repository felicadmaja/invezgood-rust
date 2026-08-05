-- Keyspace: invezgood
-- Pending order. Scrape Stockbit hanya upsert by (order_id, tahun_bulan_tanggal) (tanpa TRUNCATE).
-- tahun_bulan_tanggal = tanggal dari time_open (YYYY-MM-DD), clustering DESC.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.pending_order (
    order_id            text,
    tahun_bulan_tanggal date,
    emiten_name         text,
    status              text,
    message             text,
    side                text,
    time_open           timestamp,
    lot_open            double,
    lot_done            double,
    price_order         double,
    amount_open         double,
    amount_match        double,
    amount_match_total  double,
    is_gtc              boolean,
    updated_at          timestamp,
    PRIMARY KEY ((order_id), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_emiten_name AS
    SELECT * FROM invezgood.pending_order
    WHERE emiten_name IS NOT NULL
      AND order_id IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (emiten_name, tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, order_id ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_status AS
    SELECT * FROM invezgood.pending_order
    WHERE status IS NOT NULL
      AND order_id IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (status, tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, order_id ASC);

-- Skema aktual:
-- | column_name         | kind            | type      |
-- |---------------------|-----------------|-----------|
-- | order_id            | partition_key   | text      |
-- | tahun_bulan_tanggal | clustering DESC | date (dari time_open YYYY-MM-DD) |
-- | emiten_name         | regular         | text      |
-- | status              | regular         | text      |
-- | message             | regular         | text      |
-- | side                | regular         | text      |
-- | time_open           | regular         | timestamp |
-- | lot_open            | regular         | double    |
-- | lot_done            | regular         | double    |
-- | price_order         | regular         | double    |
-- | amount_open         | regular         | double    |
-- | amount_match        | regular         | double    |
-- | amount_match_total  | regular         | double    |
-- | is_gtc              | regular         | boolean   |
-- | updated_at          | regular         | timestamp |
--
-- Migrasi PK (order_id) → ((order_id), tahun_bulan_tanggal DESC):
-- DROP MATERIALIZED VIEW IF EXISTS invezgood.pending_order_by_emiten_name;
-- DROP MATERIALIZED VIEW IF EXISTS invezgood.pending_order_by_status;
-- DROP TABLE IF EXISTS invezgood.pending_order;
-- lalu jalankan CREATE di atas.
