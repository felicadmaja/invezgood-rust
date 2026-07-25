=== Struktur migrasi stockbit.portofolio_catatan (schema summary, kata demi kata) ===

Keyspace: "stockbit"
Replikasi (saat CREATE KEYSPACE): class SimpleStrategy, replication factor 1.

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio_catatan"
Primary key: (("emiten_name")) — "emiten_name" text.
Tidak ada clustering key.
Kolom dan tipe CQL, kata demi kata:
  "emiten_name" text — partition key
  "catatan" text
Sumber: CREATE TABLE stockbit.portofolio_catatan (emiten_name text PRIMARY KEY, catatan text); diisi aplikasi pada INSERT/UPDATE.
Kolom "emiten_name": text — kode emiten / ticker (4 huruf UPPERCASE, contoh BBCA) (PK).
Kolom "catatan": text — catatan bebas untuk emiten; kosong/null bila belum di-set.

--- Materialized view (MV) ---
Tidak ada.

--- Secondary index ---
Tidak ada.

--- Akses aplikasi ---
Crate: crate/portofolio_catatan (package proto `portofolio_catatan`).
RPC: InsertPortofolioCatatan, GetAllPortofolioCatatan, GetPortofolioCatatanByEmitenName,
     UpdatePortofolioCatatan, DeletePortofolioCatatan — semua wajib Bearer JWT.
