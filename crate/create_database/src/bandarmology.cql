=== Struktur migrasi stockbit.bandarmology (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."bandarmology"
Primary key: (("id")) — "id" uuid.
Kolom dan tipe CQL:
  "id" uuid — partition key
  "emiten_id" uuid
  "emiten_name" text
  "tahun_bulan_tanggal" date
  "agg_tahun_bulan_tanggal_emiten_name" text
  "d_1" frozen<bandarmology_day>
  "d_2" frozen<bandarmology_day>
  "d_7" frozen<bandarmology_day>
  "M_1" frozen<bandarmology_day>
  "M_3" frozen<bandarmology_day>
  "M_6" frozen<bandarmology_day>
  "M_12" frozen<bandarmology_day>

--- User-defined type (UDT) ---
(1) "stockbit"."bandarmology_top_stats" — volume bigint, percent double, rp_b bigint, acc_dist text.
(2) "stockbit"."bandarmology_broker_buy" — broker_code text, buy_volume text, buy_lot text, buy_avg bigint.
(3) "stockbit"."bandarmology_broker_sell" — broker_code text, sell_volume text, sell_lot text, sell_avg bigint.
(4) "stockbit"."bandarmology_day" — top_1/top_3/top_5/average frozen<bandarmology_top_stats>; net_volume bigint; net_value text; average_rp bigint; broker_buy list<frozen<bandarmology_broker_buy>>; broker_sell list<frozen<bandarmology_broker_sell>>.
Mapping dari sumber UI: "BY"→broker_buy, "SL"→broker_sell.

--- Materialized view (MV) ---
(1) "stockbit"."bandarmology_by_emiten_id"
SELECT "emiten_id", "id" (hanya kolom primary key MV).
WHERE "emiten_id" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "emiten_id" (uuid); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Gunakan: daftar bandarmology per emiten_id (contoh WHERE emiten_id = ?); data lengkap lewat tabel dasar WHERE id = ?.

(2) "stockbit"."bandarmology_by_agg_tahun_bulan_tanggal_emiten_name"
SELECT "agg_tahun_bulan_tanggal_emiten_name", "id" (hanya kolom primary key MV).
WHERE "agg_tahun_bulan_tanggal_emiten_name" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "agg_tahun_bulan_tanggal_emiten_name" (text); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Catatan Scylla: MV hanya boleh punya satu kolom non-PK di primary key; emiten_id tidak bisa clustering karena agg sudah memakai slot itu — gunakan id (PK tabel dasar).
Kolom "agg_tahun_bulan_tanggal_emiten_name": text — diisi aplikasi sebagai concat(tahun_bulan_tanggal, '_', emiten_name), contoh "2026-07-16_BBCA".
Gunakan: lookup per agg (contoh WHERE agg_tahun_bulan_tanggal_emiten_name = ?); data lengkap lewat tabel dasar WHERE id = ?.

=== Akhir ringkasan struktur ===
