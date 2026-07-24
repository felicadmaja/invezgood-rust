=== Struktur migrasi stockbit.wyckoff_glossary (schema summary, kata demi kata) ===

Keyspace: "stockbit"
Replikasi (saat CREATE KEYSPACE): class SimpleStrategy, replication factor 1.

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."wyckoff_glossary"
Primary key: (("name")) — "name" text.
Tidak ada clustering key.
Kolom dan tipe CQL, kata demi kata:
  "name" text — partition key
  "description" text
Sumber: CREATE TABLE stockbit.wyckoff_glossary (name text PRIMARY KEY, description text); diisi aplikasi pada INSERT/UPDATE.
Kolom "name": text — istilah / nama entri glosarium Wyckoff (PK).
Kolom "description": text — penjelasan istilah; kosong/null bila belum di-set.

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada: akses lewat PRIMARY KEY (("name")).

--- User-defined type (UDT) ---
Tidak ada.

--- Enum (tipe enum CQL) ---
Tidak ada.

=== Akhir ringkasan struktur ===
