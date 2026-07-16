=== Struktur migrasi stockbit.user (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."user"
Primary key: (("id")) — "id" uuid.
Kolom dan tipe CQL:
  "id" uuid — partition key
  "name" text
  "email" text
  "password" text — bcrypt hash (bukan plaintext)

--- Materialized view (MV) ---
(1) "stockbit"."user_by_email"
SELECT "email", "id" (hanya kolom primary key MV).
PRIMARY KEY: partition "email" (text); clustering "id" (uuid).
Gunakan: lookup user per email (WHERE email = ?); data lengkap lewat tabel dasar WHERE id = ?.

--- Seed data ---
1 baris awal: name="Felik", email="felic.admaja@gmail.com"; password di-hash bcrypt saat INSERT.

=== Akhir ringkasan struktur ===
