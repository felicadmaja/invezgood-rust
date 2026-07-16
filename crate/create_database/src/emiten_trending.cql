=== Struktur migrasi stockbit.emiten_trending (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."emiten_trending"
Primary key: (("id")) — "id" uuid.
Kolom dan tipe CQL:
  "id" uuid — partition key
  "tahun_bulan_tanggal" date
  "gainer_or_loser" text
  "emiten_id" uuid
  "emiten_name" text
  "price" text
  "value" text
  "volume" text
  "agg_tahun_bulan_tanggal_emiten_name" text
Kolom "agg_tahun_bulan_tanggal_emiten_name": text — diisi aplikasi sebagai concat(tahun_bulan_tanggal, '_', emiten_name), contoh "2026-07-16_BBCA".

--- Materialized view (MV) ---
(1) "stockbit"."emiten_trending_by_agg_tahun_bulan_tanggal_emiten_name"
SELECT "agg_tahun_bulan_tanggal_emiten_name", "id" (hanya kolom primary key MV).
WHERE "agg_tahun_bulan_tanggal_emiten_name" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "agg_tahun_bulan_tanggal_emiten_name" (text); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Gunakan: lookup per agg (contoh WHERE agg_tahun_bulan_tanggal_emiten_name = ?); data lengkap lewat tabel dasar WHERE id = ?.

(2) "stockbit"."emiten_trending_by_emiten_name"
SELECT "emiten_name", "id" (hanya kolom primary key MV).
WHERE "emiten_name" IS NOT NULL AND "id" IS NOT NULL.
PRIMARY KEY: partition "emiten_name" (text); clustering "id" (uuid).
WITH CLUSTERING ORDER BY ("id" ASC).
Gunakan: daftar trending per emiten_name (contoh WHERE emiten_name = ?); data lengkap lewat tabel dasar WHERE id = ?.

=== Akhir ringkasan struktur ===
