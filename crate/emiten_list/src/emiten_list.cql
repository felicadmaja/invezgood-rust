=== Struktur migrasi stockbit.emiten_list (schema summary, kata demi kata) ===

Keyspace: "stockbit"
Replikasi (saat CREATE KEYSPACE): class SimpleStrategy, replication factor 1.

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."emiten_list"
Primary key: (("code_name")) — "code_name" text.
Tidak ada clustering key.
Kolom dan tipe CQL, kata demi kata:
  "code_name" text — partition key
  "long_name" text
  "key_stats" map<text, text>
  "income_statement_ttm" map<text, frozen<map<text, text>>>
  "balance_sheet_quarterly" map<text, frozen<map<text, text>>>
  "cash_flow_ttm" map<text, frozen<map<text, text>>>
  "corporate_action" map<text, frozen<map<text, text>>>
  "shareholder" list<text>
  "company_profile" frozen<company_profile>
  "update_at" timestamp
Sumber: DDL bin create_emiten_list; diisi aplikasi pada INSERT.
Kolom "code_name": text — kode/nama emiten (contoh BBCA, BBRI).
Kolom "long_name": text — nama lengkap emiten (contoh Bank Central Asia Tbk).
Kolom "key_stats": map<text, text> — statistik kunci emiten (pasangan key-value).
Kolom "income_statement_ttm": map<text, frozen<map<text, text>>> — pos laporan laba rugi TTM; key luar = nama pos (contoh "Total Pendapatan"), key dalam = periode kuartal (contoh "Q1 2026"), value = nilai terformat (contoh "3,890 B").
Kolom "balance_sheet_quarterly": map<text, frozen<map<text, text>>> — pos neraca kuartalan; key luar = nama pos (contoh "Total Aset"), key dalam = periode kuartal (contoh "Q1 2026"), value = nilai terformat (contoh "3,890 B").
Kolom "cash_flow_ttm": map<text, frozen<map<text, text>>> — pos arus kas TTM; key luar = nama pos (contoh "Arus Kas Operasi"), key dalam = periode kuartal (contoh "Q1 2026"), value = nilai terformat (contoh "3,890 B").
Kolom "corporate_action": map<text, frozen<map<text, text>>> — aksi korporasi; key luar = jenis/tanggal aksi (contoh "Dividen 2026-03-15"), key dalam = atribut (contoh "ex_date", "ratio"), value = nilai terformat.
Kolom "shareholder": list<text> — daftar pemegang saham (ringkas).
Kolom "company_profile": frozen<company_profile> — profil perusahaan (lihat UDT di bawah).
Kolom "update_at": timestamp — waktu terakhir data emiten diperbarui.

--- Materialized view (MV) ---
Tidak ada: MV "stockbit"."emiten_list_by_name" (legacy) di-drop saat migrasi; lookup langsung lewat PRIMARY KEY (("code_name")).

--- Secondary index ---
Tidak ada: tidak dibuat CREATE INDEX / SAI pada "stockbit"."emiten_list"; akses lewat PRIMARY KEY (("code_name")).

--- User-defined type (UDT) ---
(1) "stockbit"."emiten_shareholder_gt1" — pemegang saham >1%: name, type, location, domicile, scriples, scrip, total_shares, percentage (semua text).
(2) "stockbit"."emiten_shareholder" — ringkasan pemegang saham: name, value, shares (semua text).
(3) "stockbit"."company_profile" — company_background text; sector text; shareholder_more_than_one_percent list<frozen<emiten_shareholder_gt1>>; shareholders list<frozen<emiten_shareholder>>; ultimate_beneficial_owner text.
Mapping dari sumber UI: "Company Background"→company_background, "Sector"→sector, "Shareholder_more_than_one_percent"→shareholder_more_than_one_percent (list), "shareholders"→shareholders, "Ultimate Beneficial Owner"→ultimate_beneficial_owner.

--- Enum (tipe enum CQL) ---
Tidak ada: tidak ada kolom bertipe enum CQL pada tabel emiten_list.

=== Akhir ringkasan struktur ===
