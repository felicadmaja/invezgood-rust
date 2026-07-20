=== Struktur migrasi stockbit.emiten_trending (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."emiten_trending"
Primary key: (("agg_tahun_bulan_tanggal_emiten_name")) — text.
Kolom dan tipe CQL:
  "agg_tahun_bulan_tanggal_emiten_name" text — partition key
  "tahun_bulan_tanggal" date
  "gainer_or_loser" text
  "emiten_name" text
  "long_name" text — nama perusahaan panjang (Redis / emiten_list / API movers)
  "emiten_icon" text
  "sector" tinyint — kode sektor (`EmitenSector`, sama `emiten_list.sector`)
  "price" double
  "price_change" double
  "value" text
  "volume" text
  "freq" text
  "updated_at" timestamp
Kolom "agg_tahun_bulan_tanggal_emiten_name": text — diisi aplikasi sebagai concat(tahun_bulan_tanggal, '_', emiten_name), contoh "2026-07-16_BBCA".
Kolom "updated_at": timestamp — waktu terakhir baris di-upsert.
Kolom "long_name": text — nama perusahaan panjang.
Kolom "emiten_icon": text — path object GCS modul stoksaham (hasil upload icon Movers).
Kolom "sector": tinyint — kode sektor emiten (sama mapping `emiten_list.sector` / EmitenSector; null = belum diisi).
Kolom "freq": text — frekuensi transaksi dari kolom Freq tabel Movers Stockbit.

--- Materialized view (MV) ---
(1) "stockbit"."emiten_trending_by_emiten_name"
SELECT "emiten_name", "agg_tahun_bulan_tanggal_emiten_name" (hanya kolom primary key MV).
WHERE "emiten_name" IS NOT NULL AND "agg_tahun_bulan_tanggal_emiten_name" IS NOT NULL.
PRIMARY KEY: partition "emiten_name" (text); clustering "agg_tahun_bulan_tanggal_emiten_name" (text).
WITH CLUSTERING ORDER BY ("agg_tahun_bulan_tanggal_emiten_name" ASC).
Gunakan: daftar trending per emiten_name (contoh WHERE emiten_name = ?); data lengkap lewat tabel dasar WHERE agg_tahun_bulan_tanggal_emiten_name = ?.

(2) "stockbit"."emiten_trending_by_tahun_bulan_tanggal"
SELECT "tahun_bulan_tanggal", "agg_tahun_bulan_tanggal_emiten_name" (hanya kolom primary key MV).
WHERE "tahun_bulan_tanggal" IS NOT NULL AND "agg_tahun_bulan_tanggal_emiten_name" IS NOT NULL.
PRIMARY KEY: partition "tahun_bulan_tanggal" (date); clustering "agg_tahun_bulan_tanggal_emiten_name" (text).
WITH CLUSTERING ORDER BY ("agg_tahun_bulan_tanggal_emiten_name" ASC).
Gunakan: daftar trending per tanggal (contoh WHERE tahun_bulan_tanggal = ?); data lengkap lewat tabel dasar WHERE agg_tahun_bulan_tanggal_emiten_name = ?.

=== Akhir ringkasan struktur ===
