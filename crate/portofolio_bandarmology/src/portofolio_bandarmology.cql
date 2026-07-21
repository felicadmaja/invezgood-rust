=== Struktur migrasi stockbit.portofolio_bandarmology (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio_bandarmology"
Primary key: (("emiten_name"), "tahun_bulan_tanggal") — partition "emiten_name" (text); clustering "tahun_bulan_tanggal" (date) DESC.
Kolom dan tipe CQL:
  "emiten_name" text — partition key; kode/nama emiten (contoh BBCA)
  "tahun_bulan_tanggal" date — clustering key DESC; tanggal snapshot bandarmology (contoh 2026-07-17)
  "bandarmology" frozen<bandarmology_day> — snapshot Bandar Detector untuk tanggal tersebut (UDT sama dengan bandarmology)

Lookup: daftar per emiten WHERE emiten_name = ? (urutan tanggal terbaru dulu);
baris tunggal WHERE emiten_name = ? AND tahun_bulan_tanggal = ?.

DDL contoh:
CREATE TABLE IF NOT EXISTS stockbit.portofolio_bandarmology (
  emiten_name text,
  tahun_bulan_tanggal date,
  bandarmology frozen<bandarmology_day>,
  PRIMARY KEY ((emiten_name), tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY ((emiten_name), tahun_bulan_tanggal).

--- User-defined type (UDT) ---
Memakai ulang UDT dari stockbit.bandarmology (tidak dibuat ulang di sini):
(1) "stockbit"."bandarmology_top_stats" — volume bigint, percent double, rp_b bigint, acc_dist text.
(2) "stockbit"."bandarmology_broker_buy" — broker_code text, buy_volume text, buy_lot text, buy_avg bigint.
(3) "stockbit"."bandarmology_broker_sell" — broker_code text, sell_volume text, sell_lot text, sell_avg bigint.
(4) "stockbit"."bandarmology_day" — top_1/top_3/top_5/average frozen<bandarmology_top_stats>; net_volume bigint; net_value text; average_rp bigint; broker_buy list<frozen<bandarmology_broker_buy>>; broker_sell list<frozen<bandarmology_broker_sell>>.

=== Akhir ringkasan struktur ===
