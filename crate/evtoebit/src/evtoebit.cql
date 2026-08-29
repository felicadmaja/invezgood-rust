-- Keyspace: invezgood
-- Median EV/EBIT & EV/EBITDA per sektor BEI (IDX-IC).
-- Sumber compute: Yahoo Finance (.JK) + kolom sector dari invezgood.stock_list.
-- Hanya baris dengan n >= EVTOEBIT_MIN_VALID_N (default 3) yang di-upsert.
-- Tabel dibuat ulang via seed (2026-08-29).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.evtoebit (
    sektor           text PRIMARY KEY,
    n                int,
    median_ev_ebit   double,
    p25_ev_ebit      double,
    p75_ev_ebit      double,
    median_ev_ebitda double,
    flag             text,
    updated_at       timestamp
);

-- Skema aktual:
-- | column_name      | kind          | type      |
-- |------------------|---------------|-----------|
-- | sektor           | partition_key | text      | nama sektor BEI dari invezgood.stock_list.sector
-- | n                | regular       | int       | jumlah emiten valid dalam sektor
-- | median_ev_ebit   | regular       | double    | median EV/EBIT (winsorize >100×)
-- | p25_ev_ebit      | regular       | double    | persentil 25 EV/EBIT
-- | p75_ev_ebit      | regular       | double    | persentil 75 EV/EBIT
-- | median_ev_ebitda | regular       | double    | median EV/EBITDA
-- | flag             | regular       | text      | "n<3" bila n < min valid; kosong bila cukup
-- | updated_at       | regular       | timestamp | waktu upsert/compute terakhir (UTC)
--
-- Query satu sektor:
-- SELECT * FROM invezgood.evtoebit WHERE sektor = 'Barang Baku';
--
-- Full scan:
-- SELECT * FROM invezgood.evtoebit;
