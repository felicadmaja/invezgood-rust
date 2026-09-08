-- Keyspace: invezgood
-- Tabel laporan keuangan kuartalan (nilai standalone setelah dekumulasi YTD).

-- CREATE TABLE IF NOT EXISTS invezgood.xlbr_laporan_keuangan (
--     code text,
--     fiscal_year int,
--     quarter text,
--     period_end timestamp,
--     presentation_currency text,
--     unit_scale int,
--     cash_from_operation double,
--     cash_from_investment double,
--     cash_from_financing double,
--     capital_expenditure double,
--     free_cash_flow double,
--     net_income double,
--     interest_paid double,      -- arus kas YTD→standalone; bunga dibayar (selalu negatif)
--     tax_paid double,           -- arus kas YTD→standalone; pajak dibayar (selalu negatif)
--     st_bank_loans double,      -- neraca CurrentYearInstant: ShorttermBankLoans/ShortTermBankLoans
--     current_maturities double, -- jumlah CurrentMaturitiesOf* (bank, obligasi, sukuk, sewa, MTN, ...)
--     lt_loans double,           -- LongTermBankLoans + pinjaman jangka panjang terkait
--     bonds double,              -- BondsPayable, LongTermBondsPayable, MTN, ConvertibleBonds, ...
--     sukuk double,              -- SukukIjarah, SukukMudharabah, LongTermSukuk, ...
--     lease_liabilities double,  -- LongTermFinanceLease*/LongTermLeaseLiabilities
--     hutang_berbunga double,    -- st_bank_loans + current_maturities + lt_loans + bonds + sukuk + lease_liabilities
// tambahkan kolom kas, nilainya diambil dari 1210000.html cari <ix:nonFraction name="idx-cor:CashAndCashEquivalents" contextRef="CurrentYearInstant" ...>
// Label baris HTML: "Kas dan setara kas" — biasanya baris pertama di aset lancar.
// Context yang kamu mau: CurrentYearInstant (posisi akhir periode pelaporan). Ada juga PriorEndYearInstant / PriorYearInstant untuk saldo pembanding (akhir tahun lalu).
// untuk crosscheck bisa di lakukan pengecekkan di 1510000.html <ix:nonFraction name="idx-cor:CashAndCashEquivalentsCashFlows" ...>
// Label: "Kas dan setara kas pada akhir periode". Nilainya seharusnya sama dengan 1210000, tapi bisa beda kalau ada kas yang direklasifikasi (misalnya kas dibatasi penggunaannya / restricted cash, atau kas dalam kelompok aset dimiliki untuk dijual). Bagus untuk validasi parser.
//     "idx-cor:CashAndCashEquivalents",           # 1210000, prioritas 1
//    "idx-cor:CashAndCashEquivalentsCashFlows",  # 1510000, fallback/validasi

--     uploaded_at timestamp,
--     source_zip_hash text,
--     catatan map<text, text>,
--     PRIMARY KEY ((code), fiscal_year, quarter)
-- ) WITH CLUSTERING ORDER BY (fiscal_year DESC, quarter DESC);

-- | column_name              | kind           | type      |
-- |--------------------------|----------------|-----------|
-- | code                     | partition_key  | text      |
-- | fiscal_year              | clustering     | int       |
-- | quarter                  | clustering     | text      | Q1..Q4
-- | period_end               | regular        | timestamp |
-- | presentation_currency    | regular        | text      |
-- | unit_scale               | regular        | int       | 3 = ribuan
-- | cash_from_operation      | regular        | double    |
-- | cash_from_investment     | regular        | double    |
-- | cash_from_financing      | regular        | double    |
-- | capital_expenditure      | regular        | double    |
-- | free_cash_flow           | regular        | double    | CFO + CapEx (CapEx negatif, standalone)
-- | net_income               | regular        | double    |
-- | interest_paid            | regular        | double    | bunga dibayar standalone (selalu negatif)
-- | tax_paid                 | regular        | double    | pajak dibayar standalone (selalu negatif)
-- | st_bank_loans            | regular        | double    | snapshot neraca per kuartal
-- | current_maturities       | regular        | double    | snapshot neraca per kuartal
-- | lt_loans                 | regular        | double    | snapshot neraca per kuartal
-- | bonds                    | regular        | double    | snapshot neraca per kuartal
-- | sukuk                    | regular        | double    | snapshot neraca per kuartal
-- | lease_liabilities        | regular        | double    | snapshot neraca per kuartal
-- | hutang_berbunga          | regular        | double    | jumlah 6 komponen di atas
-- | uploaded_at              | regular        | timestamp |
-- | source_zip_hash          | regular        | text      |
-- | catatan                  | regular        | map<text,text> |
--
-- Migrasi catatan (2026-09-06): ALTER TABLE invezgood.xlbr_laporan_keuangan ADD catatan map<text, text>;
-- Migrasi interest/tax (2026-09-07): ALTER TABLE invezgood.xlbr_laporan_keuangan ADD interest_paid double;
-- Migrasi interest/tax (2026-09-07): ALTER TABLE invezgood.xlbr_laporan_keuangan ADD tax_paid double;
-- Migrasi hutang (2026-09-08): ALTER TABLE ... ADD st_bank_loans/current_maturities/lt_loans/bonds/sukuk/lease_liabilities/hutang_berbunga double;
