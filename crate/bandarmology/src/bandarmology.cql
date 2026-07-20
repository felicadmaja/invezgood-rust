=== Struktur migrasi stockbit.bandarmology (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."bandarmology"
Primary key: (("agg_tahun_bulan_emiten_name")) — text.
Kolom dan tipe CQL:
  "agg_tahun_bulan_emiten_name" text — partition key; diisi aplikasi sebagai concat(tahun_bulan, '_', emiten_name), contoh "2026-07_BBCA"
  "emiten_name" text
  "tahun_bulan" text
  "broker_summary_current_w1" frozen<bandarmology_day> — minggu 1 bulan berjalan (tgl 1–7; dibatasi hari ini)
  "broker_summary_current_w2" frozen<bandarmology_day> — minggu 2 (tgl 8–14; null bila belum dimulai)
  "broker_summary_current_w3" frozen<bandarmology_day> — minggu 3 (tgl 15–21; null bila belum dimulai)
  "broker_summary_current_w4" frozen<bandarmology_day> — minggu 4 (tgl 22–akhir bulan; null bila belum dimulai)
  "broker_summary" frozen<bandarmology_day>

--- User-defined type (UDT) ---
(1) "stockbit"."bandarmology_top_stats" — volume bigint, percent double, rp_b bigint, acc_dist text.
(2) "stockbit"."bandarmology_broker_buy" — broker_code text, buy_volume text, buy_lot text, buy_avg bigint.
(3) "stockbit"."bandarmology_broker_sell" — broker_code text, sell_volume text, sell_lot text, sell_avg bigint.
(4) "stockbit"."bandarmology_day" — top_1/top_3/top_5/average frozen<bandarmology_top_stats>; net_volume bigint; net_value text; average_rp bigint; broker_buy list<frozen<bandarmology_broker_buy>>; broker_sell list<frozen<bandarmology_broker_sell>>.
Mapping dari sumber UI: "BY"→broker_buy, "SL"→broker_sell.

--- Materialized view (MV) ---
(1) "stockbit"."bandarmology_by_emiten_name"
SELECT "emiten_name", "agg_tahun_bulan_emiten_name" (hanya kolom primary key MV).
WHERE "emiten_name" IS NOT NULL AND "agg_tahun_bulan_emiten_name" IS NOT NULL.
PRIMARY KEY: partition "emiten_name" (text); clustering "agg_tahun_bulan_emiten_name" (text).
WITH CLUSTERING ORDER BY ("agg_tahun_bulan_emiten_name" ASC).
Gunakan: daftar bandarmology per emiten_name (contoh WHERE emiten_name = ?); data lengkap lewat tabel dasar WHERE agg_tahun_bulan_emiten_name = ?.

=== Akhir ringkasan struktur ===
