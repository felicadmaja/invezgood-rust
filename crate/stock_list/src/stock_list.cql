-- Keyspace: invezgood
-- Tabel dibuat via MCP Scylla (2026-08-02)


-- Skema aktual:
-- | column_name | kind           | type                              |
-- |-------------|----------------|-----------------------------------|
-- | code        | partition_key  | text                              |
-- | name        | regular        | text                              |
-- | sector      | regular        | text                              |
-- | logo        | regular        | text                              |
-- | keystats    | regular        | frozen<stock_list_keystats>       |
--
-- UDT stock_list_keystats:
--   rows    → list<{ id, name, values: list<{ col, year, amount, period }> }>
--   columns → list<{ year, label, period }>
