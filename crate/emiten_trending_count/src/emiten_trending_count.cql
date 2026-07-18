=== Struktur migrasi stockbit.emiten_trending_count_by_name (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."emiten_trending_count_by_name"
Primary key: (("emiten_name")) — "emiten_name" text.
Tidak ada clustering key.
Kolom dan tipe CQL:
  "emiten_name" text — partition key
  "appearance_count" bigint
  "last_tahun_bulan_tanggal" date
  "updated_at" timestamp
Kolom "emiten_name": text — kode emiten (contoh BBCA, BBRI).
Kolom "appearance_count": bigint — jumlah kemunculan emiten di tabel emiten_trending.
Kolom "last_tahun_bulan_tanggal": date — tanggal trending terakhir yang dihitung untuk emiten ini.
Kolom "updated_at": timestamp — waktu terakhir baris count diperbarui.

DDL:
CREATE TABLE IF NOT EXISTS stockbit.emiten_trending_count_by_name (
    emiten_name text,
    appearance_count bigint,
    last_tahun_bulan_tanggal date,
    updated_at timestamp,
    PRIMARY KEY ((emiten_name))
);

Gunakan: lookup count per emiten (WHERE emiten_name = ?);
TOP N via token-ring scan tabel ini lalu sort appearance_count DESC di aplikasi.

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada.

=== Akhir ringkasan struktur ===
