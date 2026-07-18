=== Struktur migrasi stockbit.broker (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."broker"
Primary key: (("broker_code")) — "broker_code" text.
Kolom dan tipe CQL:
  "broker_code" text — partition key
  "name" text
  "tipe" text
  "asosiasi" text
  "catatan" text

Lookup: WHERE broker_code = ? (contoh kode broker IDX).

--- Materialized view (MV) ---
Tidak ada: MV "stockbit"."broker_name_by_code_name" (legacy) di-drop saat migrasi; tabel legacy "stockbit"."broker_name" juga di-drop.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY (("broker_code")).

=== Akhir ringkasan struktur ===
