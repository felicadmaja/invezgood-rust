//! ```bash
//! cargo run -p create_database --bin create_bandarmology
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`), UDT bandarmology, dan tabel `bandarmology`.
//! Re-run: DROP MV + DROP TABLE + DROP TYPE lalu buat ulang (data bandarmology hilang).
//!
//! Kolom tabel (tanpa uuid):
//! - `agg_tahun_bulan_tanggal_emiten_name` text — partition key; diisi aplikasi:
//!   `concat(tahun_bulan_tanggal, '_', emiten_name)` (contoh `2026-07-16_BBCA`)
//! - `emiten_name` text
//! - `tahun_bulan_tanggal` date
//! - `d_1`, `d_2`, `d_7` frozen<bandarmology_day> — snapshot harian
//! - `M_1`, `M_3`, `M_6`, `M_12` frozen<bandarmology_day> — snapshot bulanan
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/bandarmology/src/bandarmology.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "bandarmology";
const MV_BY_EMITEN_NAME: &str = "bandarmology_by_emiten_name";
/// MV lama (uuid-era) yang di-drop pada re-run.
const LEGACY_MV_BY_EMITEN_ID: &str = "bandarmology_by_emiten_id";
const LEGACY_MV_BY_AGG: &str = "bandarmology_by_agg_tahun_bulan_tanggal_emiten_name";
const UDT_TOP_STATS: &str = "bandarmology_top_stats";
const UDT_BROKER_BUY: &str = "bandarmology_broker_buy";
const UDT_BROKER_SELL: &str = "bandarmology_broker_sell";
const UDT_DAY: &str = "bandarmology_day";

const BANDARMOLOGY_COLUMNS: &[&str] = &[
    "agg_tahun_bulan_tanggal_emiten_name",
    "emiten_name",
    "tahun_bulan_tanggal",
    "d_1",
    "d_2",
    "d_7",
    "M_1",
    "M_3",
    "M_6",
    "M_12",
];

const DAY_SNAPSHOT_COLUMNS: &[&str] = &["d_1", "d_2", "d_7", "M_1", "M_3", "M_6", "M_12"];

fn bandarmology_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../bandarmology/src/bandarmology.cql")
}

fn bandarmology_scylla_type(col: &str) -> &'static str {
    match col {
        "tahun_bulan_tanggal" => "date",
        col if DAY_SNAPSHOT_COLUMNS.contains(&col) => "frozen<bandarmology_day>",
        _ => "text",
    }
}

fn ddl_create_keyspace(keyspace: &str) -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        keyspace
    )
}

fn ddl_create_udt_top_stats(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            volume bigint, \
            percent double, \
            rp_b bigint, \
            acc_dist text\
        )",
        keyspace, UDT_TOP_STATS
    )
}

fn ddl_create_udt_broker_buy(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            broker_code text, \
            buy_volume text, \
            buy_lot text, \
            buy_avg bigint\
        )",
        keyspace, UDT_BROKER_BUY
    )
}

fn ddl_create_udt_broker_sell(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            broker_code text, \
            sell_volume text, \
            sell_lot text, \
            sell_avg bigint\
        )",
        keyspace, UDT_BROKER_SELL
    )
}

fn ddl_create_udt_day(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            top_1 frozen<{}>, \
            top_3 frozen<{}>, \
            top_5 frozen<{}>, \
            average frozen<{}>, \
            net_volume bigint, \
            net_value text, \
            average_rp bigint, \
            broker_buy list<frozen<{}>>, \
            broker_sell list<frozen<{}>>\
        )",
        keyspace,
        UDT_DAY,
        UDT_TOP_STATS,
        UDT_TOP_STATS,
        UDT_TOP_STATS,
        UDT_TOP_STATS,
        UDT_BROKER_BUY,
        UDT_BROKER_SELL
    )
}

fn ddl_drop_udt(keyspace: &str, type_name: &str) -> String {
    format!("DROP TYPE IF EXISTS {}.{}", keyspace, type_name)
}

fn ddl_drop_table(keyspace: &str) -> String {
    format!("DROP TABLE IF EXISTS {}.{}", keyspace, TABLE)
}

fn ddl_drop_materialized_view(keyspace: &str, mv: &str) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}.{}", keyspace, mv)
}

fn ddl_create_table(keyspace: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\
            \"agg_tahun_bulan_tanggal_emiten_name\" text, \
            \"emiten_name\" text, \
            \"tahun_bulan_tanggal\" date, \
            \"d_1\" frozen<{}>, \
            \"d_2\" frozen<{}>, \
            \"d_7\" frozen<{}>, \
            \"M_1\" frozen<{}>, \
            \"M_3\" frozen<{}>, \
            \"M_6\" frozen<{}>, \
            \"M_12\" frozen<{}>, \
            PRIMARY KEY ((\"agg_tahun_bulan_tanggal_emiten_name\"))\
        )",
        keyspace,
        TABLE,
        UDT_DAY,
        UDT_DAY,
        UDT_DAY,
        UDT_DAY,
        UDT_DAY,
        UDT_DAY,
        UDT_DAY
    )
}

fn ddl_create_mv_by_emiten_name(keyspace: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS \
         SELECT \"emiten_name\", \"agg_tahun_bulan_tanggal_emiten_name\" FROM {}.{} \
         WHERE \"emiten_name\" IS NOT NULL AND \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL \
         PRIMARY KEY ((\"emiten_name\"), \"agg_tahun_bulan_tanggal_emiten_name\") \
         WITH CLUSTERING ORDER BY (\"agg_tahun_bulan_tanggal_emiten_name\" ASC)",
        keyspace, MV_BY_EMITEN_NAME, keyspace, TABLE
    )
}

fn load_dotenv() {
    use std::path::PathBuf;
    let workspace_env = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
        return;
    }
    if PathBuf::from(".env").exists() {
        let _ = dotenvy::from_path(".env");
        return;
    }
    dotenvy::dotenv().ok();
}

async fn connect_session() -> Result<Session, Box<dyn std::error::Error + Send + Sync>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri.as_str());
    if let Ok(user) = std::env::var("SCYLLA_USER") {
        if let Ok(password) = std::env::var("SCYLLA_PASSWORD") {
            builder = builder.user(user, password);
        }
    }
    Ok(builder.build().await?)
}

fn format_bandarmology_schema_summary(keyspace: &str) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "=== Struktur migrasi {}.{} (schema summary) ===",
        keyspace, TABLE
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Keyspace: \"{}\"", keyspace);
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Tabel dasar (base table) ---");
    let _ = writeln!(out, "Nama penuh: \"{}\".\"{}\"", keyspace, TABLE);
    let _ = writeln!(
        out,
        "Primary key: ((\"agg_tahun_bulan_tanggal_emiten_name\")) — text."
    );
    let _ = writeln!(out, "Kolom dan tipe CQL:");
    for name in BANDARMOLOGY_COLUMNS {
        let ty = bandarmology_scylla_type(name);
        if *name == "agg_tahun_bulan_tanggal_emiten_name" {
            let _ = writeln!(
                out,
                "  \"{}\" {} — partition key; diisi aplikasi sebagai \
                 concat(tahun_bulan_tanggal, '_', emiten_name), contoh \"2026-07-16_BBCA\"",
                name, ty
            );
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "--- User-defined type (UDT) ---");
    let _ = writeln!(
        out,
        "(1) \"{}\".\"{}\" — volume bigint, percent double, rp_b bigint, acc_dist text.",
        keyspace, UDT_TOP_STATS
    );
    let _ = writeln!(
        out,
        "(2) \"{}\".\"{}\" — broker_code text, buy_volume text, buy_lot text, buy_avg bigint.",
        keyspace, UDT_BROKER_BUY
    );
    let _ = writeln!(
        out,
        "(3) \"{}\".\"{}\" — broker_code text, sell_volume text, sell_lot text, sell_avg bigint.",
        keyspace, UDT_BROKER_SELL
    );
    let _ = writeln!(
        out,
        "(4) \"{}\".\"{}\" — top_1/top_3/top_5/average frozen<{}>; \
         net_volume bigint; net_value text; average_rp bigint; \
         broker_buy list<frozen<{}>>; broker_sell list<frozen<{}>>.",
        keyspace, UDT_DAY, UDT_TOP_STATS, UDT_BROKER_BUY, UDT_BROKER_SELL
    );
    let _ = writeln!(
        out,
        "Mapping dari sumber UI: \"BY\"→broker_buy, \"SL\"→broker_sell."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Materialized view (MV) ---");
    let _ = writeln!(out, "(1) \"{}\".\"{}\"", keyspace, MV_BY_EMITEN_NAME);
    let _ = writeln!(
        out,
        "SELECT \"emiten_name\", \"agg_tahun_bulan_tanggal_emiten_name\" (hanya kolom primary key MV)."
    );
    let _ = writeln!(
        out,
        "WHERE \"emiten_name\" IS NOT NULL AND \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL."
    );
    let _ = writeln!(
        out,
        "PRIMARY KEY: partition \"emiten_name\" (text); clustering \"agg_tahun_bulan_tanggal_emiten_name\" (text)."
    );
    let _ = writeln!(
        out,
        "WITH CLUSTERING ORDER BY (\"agg_tahun_bulan_tanggal_emiten_name\" ASC)."
    );
    let _ = writeln!(
        out,
        "Gunakan: daftar bandarmology per emiten_name (contoh WHERE emiten_name = ?); \
         data lengkap lewat tabel dasar WHERE agg_tahun_bulan_tanggal_emiten_name = ?."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_bandarmology_schema_summary(keyspace: &str) {
    let text = format_bandarmology_schema_summary(keyspace);
    eprint!("\n{text}");
    if let Some(parent) = bandarmology_cql_output_path().as_path().parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "Peringatan: gagal membuat direktori {}: {}",
                parent.display(),
                e
            );
        }
    }
    match std::fs::write(bandarmology_cql_output_path(), &text) {
        Ok(()) => eprintln!(
            "OK: ringkasan skema ditulis ke {}",
            bandarmology_cql_output_path().display()
        ),
        Err(e) => eprintln!(
            "Peringatan: gagal menulis {}: {}",
            bandarmology_cql_output_path().display(),
            e
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    load_dotenv();

    let keyspace = std::env::var("SCYLLA_KEYSPACE")
        .map_err(|_| "SCYLLA_KEYSPACE wajib diisi di .env")?;

    let session = connect_session().await?;

    let ddl_keyspace = ddl_create_keyspace(&keyspace);
    session.query_unpaged(ddl_keyspace.as_str(), &[]).await?;
    eprintln!("OK: CREATE KEYSPACE IF NOT EXISTS {keyspace}");

    for mv in [LEGACY_MV_BY_EMITEN_ID, LEGACY_MV_BY_AGG, MV_BY_EMITEN_NAME] {
        let ddl_drop_mv = ddl_drop_materialized_view(&keyspace, mv);
        session.query_unpaged(ddl_drop_mv.as_str(), &[]).await?;
        eprintln!("OK: {ddl_drop_mv}");
    }

    let ddl_drop_table = ddl_drop_table(&keyspace);
    session.query_unpaged(ddl_drop_table.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_table}");

    for type_name in [UDT_DAY, UDT_BROKER_BUY, UDT_BROKER_SELL, UDT_TOP_STATS] {
        let ddl = ddl_drop_udt(&keyspace, type_name);
        session.query_unpaged(ddl.as_str(), &[]).await?;
        eprintln!("OK: {ddl}");
    }

    let ddl_top = ddl_create_udt_top_stats(&keyspace);
    session.query_unpaged(ddl_top.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_TOP_STATS}");

    let ddl_buy = ddl_create_udt_broker_buy(&keyspace);
    session.query_unpaged(ddl_buy.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_BROKER_BUY}");

    let ddl_sell = ddl_create_udt_broker_sell(&keyspace);
    session.query_unpaged(ddl_sell.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_BROKER_SELL}");

    let ddl_day = ddl_create_udt_day(&keyspace);
    session.query_unpaged(ddl_day.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_DAY}");

    let ddl_table = ddl_create_table(&keyspace);
    session.query_unpaged(ddl_table.as_str(), &[]).await?;
    eprintln!("OK: CREATE TABLE {keyspace}.{TABLE}");

    let ddl_mv_emiten = ddl_create_mv_by_emiten_name(&keyspace);
    session.query_unpaged(ddl_mv_emiten.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_EMITEN_NAME}");

    eprintln_bandarmology_schema_summary(&keyspace);

    Ok(())
}
