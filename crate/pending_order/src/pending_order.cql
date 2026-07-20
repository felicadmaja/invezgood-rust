=== Struktur migrasi stockbit.pending_order (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."pending_order"
Primary key: (("order_id")) — "order_id" text.
Kolom dan tipe CQL:
  "order_id" text — partition key
  "emiten_name" text
  "status" text
  "side" text
  "time_open" date
  "lot_open" double
  "lot_done" double
  "price_order" double
  "amount_open" double
  "amount_match" double
  "amount_match_total" double
  "is_gtc" boolean

Lookup: WHERE order_id = ?.

--- Materialized view (MV) ---
(1) "stockbit"."pending_order_by_emiten_name"
SELECT * FROM "stockbit"."pending_order"
WHERE "emiten_name" IS NOT NULL AND "order_id" IS NOT NULL.
PRIMARY KEY: partition "emiten_name" (text); clustering "order_id" (text).
WITH CLUSTERING ORDER BY ("order_id" ASC).
Gunakan: daftar pending order per emiten_name (contoh WHERE emiten_name = ?); baris tunggal lewat tabel dasar WHERE order_id = ?.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY (("order_id")) atau MV by emiten_name.

=== Akhir ringkasan struktur ===
