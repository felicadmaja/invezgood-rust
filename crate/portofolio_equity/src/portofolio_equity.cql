=== Struktur migrasi stockbit.portofolio_equity (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio_equity"
Primary key: (("nama")) — "nama" text.
Kolom dan tipe CQL:
  "nama" text — partition key
  "value" double

Lookup: WHERE nama = ?.

=== Akhir ringkasan struktur ===
