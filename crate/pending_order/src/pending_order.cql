-- Keyspace: invezgood
-- Pending order. Scrape Stockbit hanya upsert by (order_id, tahun_bulan_tanggal)
-- tahun_bulan_tanggal = tanggal invoke scrape/API (waktu lokal server YYYY-MM-DD), bukan dari time_open.

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.pending_order (
    order_id            text,
    tahun_bulan_tanggal date,
    tahun_bulan         text,
    emiten_name         text,
    emiten_name_status  text,
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

-- PK emiten_name_status = emiten_name + '_' + status (contoh ANTM_OPEN).
CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_emiten_name_status AS
    SELECT * FROM invezgood.pending_order
    WHERE emiten_name_status IS NOT NULL
      AND order_id IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (emiten_name_status, tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, order_id ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_status AS
    SELECT * FROM invezgood.pending_order
    WHERE status IS NOT NULL
      AND order_id IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (status, tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, order_id ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_tahun_bulan_tanggal AS
    SELECT * FROM invezgood.pending_order
    WHERE tahun_bulan_tanggal IS NOT NULL
      AND order_id IS NOT NULL
    PRIMARY KEY (tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (order_id ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.pending_order_by_tahun_bulan AS
    SELECT * FROM invezgood.pending_order
    WHERE tahun_bulan IS NOT NULL
      AND order_id IS NOT NULL
      AND tahun_bulan_tanggal IS NOT NULL
    PRIMARY KEY (tahun_bulan, tahun_bulan_tanggal, order_id)
    WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC, order_id ASC);

-- Skema aktual:
-- | column_name         | kind            | type      |
-- |---------------------|-----------------|-----------|
-- | order_id            | partition_key   | text      |
-- | tahun_bulan_tanggal | clustering DESC | date (tanggal invoke scrape/API, waktu lokal) |
-- | tahun_bulan         | regular         | text (YYYY-MM dari tahun_bulan_tanggal) |
-- | emiten_name         | regular         | text      |
-- | emiten_name_status  | regular         | text (emiten_name + '_' + status) |
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
-- MV pending_order_by_tahun_bulan:
-- PK (tahun_bulan, tahun_bulan_tanggal DESC, order_id ASC)
-- Query: WHERE tahun_bulan = 'YYYY-MM'
--
-- MV pending_order_by_emiten_name_status:
-- PK (emiten_name_status, tahun_bulan_tanggal DESC, order_id ASC)
-- Query: WHERE emiten_name_status = 'ANTM_OPEN'
--
-- Migrasi emiten_name_status (jalan sekali bila tabel sudah ada):
-- ALTER TABLE invezgood.pending_order ADD emiten_name_status text;
-- UPDATE invezgood.pending_order SET emiten_name_status = emiten_name + '_' + status
--   WHERE order_id = ? AND tahun_bulan_tanggal = ?;  -- ulangi / batch backfill
-- CREATE MATERIALIZED VIEW pending_order_by_emiten_name_status ... (lihat di atas).
--
-- Migrasi (jalan sekali bila tabel sudah ada tanpa tahun_bulan):
-- ALTER TABLE invezgood.pending_order ADD tahun_bulan text;
-- UPDATE ... SET tahun_bulan = 'YYYY-MM' WHERE order_id = ? AND tahun_bulan_tanggal = ?;
-- lalu CREATE MATERIALIZED VIEW pending_order_by_tahun_bulan di atas.
--
-- Migrasi PK (order_id) → ((order_id), tahun_bulan_tanggal DESC):
-- DROP MATERIALIZED VIEW IF EXISTS invezgood.pending_order_by_emiten_name;
-- DROP MATERIALIZED VIEW IF EXISTS invezgood.pending_order_by_status;
-- DROP MATERIALIZED VIEW IF EXISTS invezgood.pending_order_by_tahun_bulan_tanggal;
-- DROP TABLE IF EXISTS invezgood.pending_order;
-- lalu jalankan CREATE di atas.
