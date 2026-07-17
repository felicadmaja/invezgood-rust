=== Struktur migrasi stockbit.catatan (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."catatan"
Primary key: (("agg_tahun_bulan_tanggal_emiten_name")) — text.
Kolom dan tipe CQL:
  "agg_tahun_bulan_tanggal_emiten_name" text — partition key
  "tahun_bulan_tanggal" date
  "emiten_name" text
  "catatan" text
Kolom "agg_tahun_bulan_tanggal_emiten_name": text — diisi aplikasi sebagai concat(tahun_bulan_tanggal, '_', emiten_name), contoh "2026-07-17_BBCA".
Kolom "tahun_bulan_tanggal": date — tanggal catatan.
Kolom "emiten_name": text — kode emiten (contoh BBCA).
Kolom "catatan": text — isi catatan.

Gunakan: lookup catatan per agg (contoh WHERE agg_tahun_bulan_tanggal_emiten_name = ?).

=== Akhir ringkasan struktur ===
