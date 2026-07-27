=== Struktur migrasi stockbit.bandarmology_harian (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."bandarmology_harian"
Primary key: (("emiten_name"), "tahun_bulan_tanggal") — partition "emiten_name" text; clustering "tahun_bulan_tanggal" date DESC.
Kolom dan tipe CQL:
  "emiten_name" text — partition key; kode emiten (contoh BBCA)
  "tahun_bulan_tanggal" date — clustering key DESC; tanggal snapshot harian
  "broker_summary_harian" frozen<bandarmology_day> — ringkasan Bandar Detector untuk hari itu (UDT sama `bandarmology`)
  "updated_at" timestamp — diisi aplikasi saat upsert (UTC now)
Sumber: CREATE TABLE stockbit.bandarmology_harian (...); UDT `bandarmology_day` sudah ada (lihat bandarmology.cql).

--- User-defined type (UDT) ---
Memakai UDT yang sama dengan tabel `bandarmology`:
(1) "stockbit"."bandarmology_top_stats"
(2) "stockbit"."bandarmology_broker_buy"
(3) "stockbit"."bandarmology_broker_sell"
(4) "stockbit"."bandarmology_day"

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY (("emiten_name"), "tahun_bulan_tanggal").

=== Akhir ringkasan struktur ===
