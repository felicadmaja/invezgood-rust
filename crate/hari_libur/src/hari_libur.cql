-- Keyspace: invezgood 
-- Kalender libur nasional + cuti bersama Indonesia (PK date).
-- Sumber: GET https://api.kemendesa.link/libur-nasional/api/holidays/{tahun}.json
--   response: { metadata: {...}, data: [ { date, name, is_civic, is_religious, is_cuti_bersama } ] }
-- Tabel dibuat via MCP Scylla (2026-08-17).

CREATE TABLE IF NOT EXISTS invezgood.hari_libur (
    date            date PRIMARY KEY,
    tahun           text,
    name            text,
    is_civic        boolean,
    is_religious    boolean,
    is_cuti_bersama boolean,
    updated_at      timestamp
);

-- MV untuk ambil semua libur satu tahun dalam satu partisi.
CREATE MATERIALIZED VIEW IF NOT EXISTS invezgood.hari_libur_by_tahun AS
    SELECT tahun, date, name, is_civic, is_religious, is_cuti_bersama, updated_at
    FROM invezgood.hari_libur
    WHERE tahun IS NOT NULL AND date IS NOT NULL
    PRIMARY KEY (tahun, date)
    WITH CLUSTERING ORDER BY (date ASC);

-- Skema aktual:
-- | column_name     | kind          | type      |
-- |-----------------|---------------|-----------|
-- | date            | partition_key | date      | tanggal libur (YYYY-MM-DD)
-- | tahun           | regular       | text      | 'YYYY' dari date; partition key MV hari_libur_by_tahun
-- | name            | regular       | text      | nama libur, mis. "Idul Fitri 1447 Hijriah"
-- | is_civic        | regular       | boolean   | libur kenegaraan/nasional
-- | is_religious    | regular       | boolean   | libur keagamaan
-- | is_cuti_bersama | regular       | boolean   | cuti bersama (bukan libur nasional murni)
-- | updated_at      | regular       | timestamp | waktu upsert terakhir dari API

-- Query per tanggal (cek hari libur):
-- SELECT * FROM invezgood.hari_libur WHERE date = '2026-08-17';
--
-- Satu tahun lewat MV (1 partisi, urut tanggal):
-- SELECT * FROM invezgood.hari_libur_by_tahun WHERE tahun = '2026';
--
-- Semua tahun:
-- SELECT * FROM invezgood.hari_libur;
--
-- Data 2026 (25 baris, SKB 3 Menteri 2026) sudah di-import 2026-08-17 dari
-- api.kemendesa.link/libur-nasional/api/holidays/2026.json.
