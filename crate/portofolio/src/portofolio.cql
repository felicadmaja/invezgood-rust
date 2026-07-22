=== Struktur migrasi stockbit.portofolio (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."portofolio"
Primary key: (("emiten_name")) — "emiten_name" text.
Tidak ada clustering key.
Kolom dan tipe CQL:
  "emiten_name" text — partition key
  "long_name" text — nama perusahaan panjang (dari emiten_list / API)
  "emiten_icon" text
  "balance_lot" bigint
  "available_lot" bigint
  "average_price" double
  "current_price" double
  "invested" double
  "market_value" double
  "potential_p_l" double
  "percentage" double

Catatan: kolom `history` dihapus dari tabel ini; riwayat order disimpan di
`stockbit.portofolio_history` (lihat `crate/portofolio_history`).

--- UDT ---
(1) "stockbit"."portofolio_history_item" — masih dipakai oleh tabel `portofolio_history`
  (bukan kolom tabel `portofolio` lagi):
  "order_id" text
  "message" text
  "symbol" text
  "side" text
  "lot_done" int
  "price_average" double
  "amount_matched" double

--- Materialized view (MV) ---
Tidak ada: MV "stockbit"."portofolio_by_emiten_name" (legacy) di-drop saat migrasi; lookup langsung lewat PRIMARY KEY (("emiten_name")).

=== Akhir ringkasan struktur ===
