-- Keyspace: stockbit (tabel sudah ada di cluster; dokumentasi skema untuk crate bandarmology)

CREATE TYPE IF NOT EXISTS stockbit.bandarmology_top_stats (
    volume   bigint,
    percent  double,
    rp_b     bigint,
    acc_dist text
);

CREATE TYPE IF NOT EXISTS stockbit.bandarmology_broker_buy (
    broker_code text,
    buy_volume  text,
    buy_lot     text,
    buy_avg     bigint
);

CREATE TYPE IF NOT EXISTS stockbit.bandarmology_broker_sell (
    broker_code  text,
    sell_volume  text,
    sell_lot     text,
    sell_avg     bigint
);

CREATE TYPE IF NOT EXISTS stockbit.bandarmology_day (
    top_1       frozen<bandarmology_top_stats>,
    top_3       frozen<bandarmology_top_stats>,
    top_5       frozen<bandarmology_top_stats>,
    average     frozen<bandarmology_top_stats>,
    net_volume  bigint,
    net_value   text,
    average_rp  bigint,
    broker_buy  frozen<list<frozen<bandarmology_broker_buy>>>,
    broker_sell frozen<list<frozen<bandarmology_broker_sell>>>
);

CREATE TABLE IF NOT EXISTS stockbit.bandarmology (
    agg_tahun_bulan_emiten_name text PRIMARY KEY,
    emiten_name                 text,
    tahun_bulan                 text,
    broker_summary              frozen<bandarmology_day>,
    broker_summary_current_w1   frozen<bandarmology_day>,
    broker_summary_current_w2   frozen<bandarmology_day>,
    broker_summary_current_w3   frozen<bandarmology_day>,
    broker_summary_current_w4   frozen<bandarmology_day>,
    updated_at                  timestamp
);

CREATE TABLE IF NOT EXISTS stockbit.bandarmology_harian (
    emiten_name           text,
    tahun_bulan_tanggal   date,
    broker_summary_harian frozen<bandarmology_day>,
    updated_at            timestamp,
    PRIMARY KEY (emiten_name, tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);

CREATE TABLE IF NOT EXISTS stockbit.portofolio_bandarmology (
    emiten_name         text,
    tahun_bulan_tanggal date,
    bandarmology        frozen<bandarmology_day>,
    PRIMARY KEY (emiten_name, tahun_bulan_tanggal)
) WITH CLUSTERING ORDER BY (tahun_bulan_tanggal DESC);
