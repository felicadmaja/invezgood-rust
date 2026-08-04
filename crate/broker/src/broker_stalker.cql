-- Keyspace: invezgood
-- Cache/hasil Broker Stalker Invezgo per broker + bulan (YYYY-MM).

CREATE TABLE IF NOT EXISTS invezgood.broker_stalker (
    broker_code text,
    tahun_bulan text,
    summary     map<text, text>,
    list        list<frozen<map<text, text>>>,
    PRIMARY KEY ((broker_code), tahun_bulan)
) WITH CLUSTERING ORDER BY (tahun_bulan DESC);

-- Skema:
-- | column_name | kind           | type                          |
-- |-------------|----------------|-------------------------------|
-- | broker_code | partition_key  | text                          |
-- | tahun_bulan | clustering DESC| text (YYYY-MM)                |
-- | summary     | regular        | map<text, text>               |
-- | list        | regular        | list<frozen<map<text, text>>> |
--
-- Catatan: elemen list memakai frozen map (syarat CQL untuk collection bersarang).
