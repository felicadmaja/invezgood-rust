=== Struktur migrasi stockbit.portofolio_history (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio_history"
Primary key: (("emiten_name"), "tahun_bulan_tanggal") — partition "emiten_name" (text); clustering "tahun_bulan_tanggal" (date) DESC.
Kolom dan tipe CQL:
  "emiten_name" text — partition key; kode/nama emiten (contoh BBCA)
  "tahun_bulan_tanggal" date — clustering key DESC; tanggal snapshot history (contoh 2026-07-22)
  "history" map<timestamp, frozen<portofolio_history_item>> — riwayat matched order hari itu;
    key = waktu order (timestamp ISO, contoh 2026-07-20T07:20:55Z);
    value = UDT `portofolio_history_item`

Lookup: daftar per emiten WHERE emiten_name = ? (urutan tanggal terbaru dulu);
baris tunggal WHERE emiten_name = ? AND tahun_bulan_tanggal = ?.

DDL contoh:
CREATE TABLE IF NOT EXISTS stockbit.portofolio_history (
  emiten_name text,
  tahun_bulan_tanggal date,
  history map<timestamp, frozen<portofolio_history_item>>,
  PRIMARY KEY ((emiten_name), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY ((emiten_name), tahun_bulan_tanggal).

--- User-defined type (UDT) ---
Memakai ulang UDT dari stockbit.portofolio (tidak dibuat ulang di sini):
(1) "stockbit"."portofolio_history_item"
  "order_id" text
  "message" text
  "symbol" text
  "side" text
  "lot_done" int
  "price_average" double
  "amount_matched" double

=== Akhir ringkasan struktur ===
