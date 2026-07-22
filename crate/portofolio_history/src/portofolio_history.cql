=== Struktur migrasi stockbit.portofolio_history (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio_history"
Primary key: (("emiten_name"), "tahun_bulan_tanggal") — partition "emiten_name" (text); clustering "tahun_bulan_tanggal" (date) DESC.
Kolom dan tipe CQL:
  "emiten_name" text — partition key; kode/nama emiten (contoh BBCA)
  "tahun_bulan_tanggal" date — clustering key DESC; tanggal transaksi dari API `date`
    (contoh "20 Jul 2026" → 2026-07-20)
  "history" list<frozen<portofolio_history_item>> — riwayat transaksi pada tanggal itu;
    elemen = UDT `portofolio_history_item` (urutan seperti `history_list` API `/history`)

Lookup: daftar per emiten WHERE emiten_name = ? (urutan tanggal terbaru dulu);
baris tunggal WHERE emiten_name = ? AND tahun_bulan_tanggal = ?.

DDL contoh:
CREATE TABLE IF NOT EXISTS stockbit.portofolio_history (
  emiten_name text,
  tahun_bulan_tanggal date,
  history list<frozen<portofolio_history_item>>,
  PRIMARY KEY ((emiten_name), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY ((emiten_name), tahun_bulan_tanggal).

--- User-defined type (UDT) ---
Memakai ulang UDT dari stockbit.portofolio (tidak dibuat ulang di sini):
(1) "stockbit"."portofolio_history_item"
  "command" text
  "symbol" text
  "price" double
  "lot" double
  "amount" double
  "status" text

=== Akhir ringkasan struktur ===
