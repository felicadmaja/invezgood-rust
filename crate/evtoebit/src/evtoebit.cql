-- Keyspace: invezgood
-- Median EV/EBIT & EV/EBITDA per sektor/sub-sektor BEI (IDX-IC).
-- Sumber compute: Yahoo Finance (.JK) + pemetaan sektor dari invezgood.stock_list.
-- Hanya baris dengan n >= EVTOEBIT_MIN_VALID_N (default 5) yang di-upsert.
-- Tabel dibuat via MCP Scylla (2026-08-29).

CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS invezgood.evtoebit (
    sektor           text,
    sub_sektor       text,
    n                int,
    median_ev_ebit   double,
    p25_ev_ebit      double,
    p75_ev_ebit      double,
    median_ev_ebitda double,
    flag             text,
    updated_at       timestamp,
    PRIMARY KEY (sektor, sub_sektor)
);

-- Skema aktual:
-- | column_name      | kind            | type      |
-- |------------------|-----------------|-----------|
-- | sektor           | partition_key   | text      | nama sektor BEI, mis. "Barang Baku"
-- | sub_sektor       | clustering_key  | text      | sub-sektor BEI atau "(agregat sektor)"
-- | n                | regular         | int       | jumlah emiten valid dalam grup
-- | median_ev_ebit   | regular         | double    | median EV/EBIT (winsorize >100×)
-- | p25_ev_ebit      | regular         | double    | persentil 25 EV/EBIT
-- | p75_ev_ebit      | regular         | double    | persentil 75 EV/EBIT
-- | median_ev_ebitda | regular         | double    | median EV/EBITDA
-- | flag             | regular         | text      | "n<5" bila n < min valid; kosong bila cukup
-- | updated_at       | regular         | timestamp | waktu upsert/compute terakhir (UTC)
--
-- Query satu baris:
-- SELECT * FROM invezgood.evtoebit WHERE sektor = 'Barang Baku' AND sub_sektor = 'Barang Baku';
--
-- Semua baris satu sektor:
-- SELECT * FROM invezgood.evtoebit WHERE sektor = 'Energi';
--
-- Full scan (jumlah baris kecil):
-- SELECT * FROM invezgood.evtoebit;
