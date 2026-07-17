=== Struktur migrasi stockbit.portofolio (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio"
Primary key: (("id")) — "id" uuid.
Kolom dan tipe CQL:
  "id" uuid — partition key
  "emiten_name" text
  "balance_lot" bigint
  "available_lot" bigint
  "average_price" double
  "current_price" double
  "invested" double
  "market_value" double
  "potential_p_l" double
  "percentage" double

--- Materialized view (MV) ---
(1) "stockbit"."portofolio_by_emiten_name"
SELECT "emiten_name", "id" (hanya kolom primary key MV).
WHERE "emiten_name" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "emiten_name" (text); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Gunakan: daftar portofolio per emiten_name (contoh WHERE emiten_name = ?); data lengkap lewat tabel dasar WHERE id = ?.

=== Akhir ringkasan struktur ===
