=== Struktur migrasi stockbit.broker_name (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."broker_name"
Primary key: (("id")) — "id" uuid.
Kolom dan tipe CQL:
  "id" uuid — partition key
  "code_name" text
  "name" text
  "tipe" text

--- Materialized view (MV) ---
(1) "stockbit"."broker_name_by_code_name"
SELECT "code_name", "id" (hanya kolom primary key MV).
WHERE "code_name" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "code_name" (text); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Gunakan: lookup broker per code_name (contoh WHERE code_name = ?); data lengkap lewat tabel dasar WHERE id = ?.

=== Akhir ringkasan struktur ===
