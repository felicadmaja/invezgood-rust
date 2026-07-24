=== Struktur migrasi stockbit.emiten_list (schema summary, kata demi kata) ===

Keyspace: "stockbit"
Replikasi (saat CREATE KEYSPACE): class SimpleStrategy, replication factor 1.

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."emiten_list"
Primary key: (("emiten_name")) — "emiten_name" text.
Tidak ada clustering key.
Kolom dan tipe CQL, kata demi kata:
  "emiten_name" text — partition key
  "long_name" text
  "emiten_icon" text
  "key_stats" map<text, text>
  "corporate_action" list<frozen<map<text, frozen<list<frozen<map<text, text>>>>>>>
  "company_profile" frozen<company_profile>
  "update_at" timestamp
  "is_konglomerasi" boolean
  "sector" tinyint
  "is_fundamental_solid" boolean
  "is_blue_chip" boolean
  "catatan" map<text, text>
  "catatan_owner" text
  "foto_owner" list<text>
  "net_income" map<text, frozen<map<text, text>>>
  "is_plan_to_trade" boolean
  "takeprofit_wyckoff" map<text, text>
  "wyckoff_phase_element" map<text, frozen<list<text>>>
Sumber: DDL bin create_emiten_list + ALTER TABLE (...); wyckoff_phase_element awalnya map<text, frozen<list<int>>> lalu diubah ke list<text> (DROP table + restore snapshot + ADD, karena Scylla menolak reuse nama collection dengan tipe berbeda); diisi aplikasi pada INSERT/UPDATE.
Kolom "emiten_name": text — kode/nama emiten (contoh BBCA, BBRI).
Kolom "long_name": text — nama lengkap emiten (contoh Bank Central Asia Tbk).
Kolom "emiten_icon": text — path object GCS modul stoksaham (mis. stoksaham/icon/BBCA.png).
Kolom "key_stats": map<text, text> — statistik kunci emiten (pasangan key-value); contoh dari UI Key Stats: "Market Cap", "Enterprise Value", "Current Share Outstanding", "Free Float", Income Statement ("Revenue (TTM)", "Gross Profit (TTM)", "EBITDA (TTM)", "Net Income (TTM)"), Solvency ("Current Ratio (Quarter)", "Debt to Equity Ratio (Quarter)", dll.), margin Profitability, Cash Flow Statement ("Cash From Operations (TTM)", "Cash From Investing (TTM)", "Cash From Financing (TTM)", "Capital expenditure (TTM)", "Free cash flow (TTM)"), dll.
Kolom "net_income": map<text, frozen<map<text, text>>> — laba bersih per tahun → periode → nilai (teks UI). Outer key = tahun ("2026"); inner key = "Q1"|"Q2"|"Q3"|"Q4"|"Annualised"|"TTM (Q1)" (dll.); value contoh "1,387 B" atau "-".
Kolom "corporate_action": list<frozen<map<text, frozen<list<frozen<map<text, text>>>>>>> — aksi korporasi sebagai list berurutan; tiap elemen map jenis aksi → list atribut; contoh [{ "Dividend": [{ "Dividend": "Rp 209" }, { "Cum Date": "20 Apr 26" }, { "Ex Date": "21 Apr 26" }, { "Recording Date": "22 Apr 26" }, { "Payment Date": "8 Mei 2026" }] }, ...].
Kolom "company_profile": frozen<company_profile> — profil perusahaan (lihat UDT di bawah).
Kolom "sector": tinyint — kode sektor emiten (`EmitenSector` proto, 0 = UNSPECIFIED); null dibaca sebagai 0.
Kolom "is_konglomerasi": boolean — true bila emiten termasuk konglomerasi; default false.
Kolom "is_fundamental_solid": boolean — true bila fundamental solid; default false.
Kolom "is_blue_chip": boolean — true bila blue chip; default false.
Kolom "is_plan_to_trade": boolean — true bila direncanakan untuk trade; default false.
Kolom "takeprofit_wyckoff": map<text, text> — take-profit / Wyckoff; key: n_kolom, harga_xo_frequent, low, takeprofit_1, takeprofit_2 (value teks angka); kosong/null bila belum di-set.
Kolom "wyckoff_phase_element": map<text, frozen<list<text>>> — fase/elemen Wyckoff: key teks → list teks; contoh {"accumulation": ["1", "2", "3"], "markup": ["10", "20"]}; kosong/null bila belum di-set.
Kolom "catatan": map<text, text> — catatan manual (pasangan key-value); kosong/{} bila belum di-set.
Kolom "catatan_owner": text — pemilik/penulis catatan (kosong bila belum di-set).
Kolom "foto_owner": list<text> — daftar path/URL foto pemilik catatan (kosong/[] bila belum di-set).
Kolom "update_at": timestamp — waktu terakhir data emiten diperbarui.

--- Materialized view (MV) ---
(1) "stockbit"."emiten_list_by_is_blue_chip" — base "emiten_list"; PRIMARY KEY (("is_blue_chip"), "emiten_name"); CLUSTERING ORDER BY ("emiten_name" ASC); SELECT *; WHERE is_blue_chip IS NOT NULL AND emiten_name IS NOT NULL. Lookup daftar blue chip: WHERE is_blue_chip = true.
Legacy "emiten_list_by_name" di-drop saat migrasi; lookup by ticker tetap lewat PRIMARY KEY base (("emiten_name")).

--- Secondary index ---
Tidak ada: tidak dibuat CREATE INDEX / SAI pada "stockbit"."emiten_list"; akses lewat PRIMARY KEY (("emiten_name")).

--- User-defined type (UDT) ---
(1) "stockbit"."emiten_shareholder_gt1" — pemegang saham >1%: name, type, location, domicile, scriples, scrip, total_shares, percentage (semua text).
(2) "stockbit"."emiten_shareholder" — ringkasan pemegang saham: name, value, shares (semua text).
(3) "stockbit"."company_profile" — company_background text; sector text; shareholder_more_than_one_percent list<frozen<emiten_shareholder_gt1>>; shareholders list<frozen<emiten_shareholder>>; ultimate_beneficial_owner text.
Mapping dari sumber UI: "Company Background"→company_background, "Sector"→sector, "Shareholder_more_than_one_percent"→shareholder_more_than_one_percent (list), "shareholders"→shareholders, "Ultimate Beneficial Owner"→ultimate_beneficial_owner.

--- Enum (tipe enum CQL) ---
Tidak ada: tidak ada kolom bertipe enum CQL pada tabel emiten_list.

=== Akhir ringkasan struktur ===
