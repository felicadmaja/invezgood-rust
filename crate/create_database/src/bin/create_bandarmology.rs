//! ```bash
//! cargo run -p create_database --bin create_bandarmology
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`), UDT bandarmology, dan tabel `bandarmology`.
//! Re-run: DROP MV + DROP TABLE + DROP TYPE lalu buat ulang (data bandarmology hilang).
//!
//! Kolom tabel:
//! - `id` uuid — partition key
//! - `emiten_id` uuid
//! - `emiten_name` text
//! - `d_1`, `d_2`, `d_7` frozen<bandarmology_day> — snapshot harian
//! - `M_1`, `M_3`, `M_6`, `M_12` frozen<bandarmology_day> — snapshot bulanan
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`src/bandarmology.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "bandarmology";
const MV_BY_EMITEN_ID: &str = "bandarmology_by_emiten_id";
const MV_BY_AGG: &str = "bandarmology_by_agg_tahun_bulan_tanggal_emiten_name";
const UDT_TOP_STATS: &str = "bandarmology_top_stats";
const UDT_BROKER_BUY: &str = "bandarmology_broker_buy";
const UDT_BROKER_SELL: &str = "bandarmology_broker_sell";
const UDT_DAY: &str = "bandarmology_day";

const BANDARMOLOGY_COLUMNS: &[&str] = &[
    "id",
    "emiten_id",
    "emiten_name",
    "tahun_bulan_tanggal",
    "agg_tahun_bulan_tanggal_emiten_name",
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
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/bandarmology.cql")
}

fn bandarmology_scylla_type(col: &str) -> &'static str {
    match col {
        "id" | "emiten_id" => "uuid",
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

fn ddl_create_mv_by_emiten_id(keyspace: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS \
         SELECT \"emiten_id\", \"id\" FROM {}.{} \
         WHERE \"emiten_id\" IS NOT NULL AND \"id\" IS NOT NULL \
         PRIMARY KEY ((\"emiten_id\"), \"id\") \
         WITH CLUSTERING ORDER BY (\"id\" ASC)",
        keyspace, MV_BY_EMITEN_ID, keyspace, TABLE
    )
}

fn ddl_create_mv_by_agg(keyspace: &str) -> String {
    // Scylla: hanya satu kolom non-PK boleh masuk MV primary key — partition agg,
    // clustering id (PK tabel dasar). emiten_id tidak bisa jadi clustering karena sudah ada agg.
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS \
         SELECT \"agg_tahun_bulan_tanggal_emiten_name\", \"id\" FROM {}.{} \
         WHERE \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL AND \"id\" IS NOT NULL \
         PRIMARY KEY ((\"agg_tahun_bulan_tanggal_emiten_name\"), \"id\") \
         WITH CLUSTERING ORDER BY (\"id\" ASC)",
        keyspace, MV_BY_AGG, keyspace, TABLE
    )
}

fn ddl_create_table(keyspace: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\
            \"id\" uuid, \
            \"emiten_id\" uuid, \
            \"emiten_name\" text, \
            \"tahun_bulan_tanggal\" date, \
            \"agg_tahun_bulan_tanggal_emiten_name\" text, \
            \"d_1\" frozen<{}>, \
            \"d_2\" frozen<{}>, \
            \"d_7\" frozen<{}>, \
            \"M_1\" frozen<{}>, \
            \"M_3\" frozen<{}>, \
            \"M_6\" frozen<{}>, \
            \"M_12\" frozen<{}>, \
            PRIMARY KEY ((\"id\"))\
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
    let _ = writeln!(out, "Primary key: ((\"id\")) — \"id\" uuid.");
    let _ = writeln!(out, "Kolom dan tipe CQL:");
    for name in BANDARMOLOGY_COLUMNS {
        let ty = bandarmology_scylla_type(name);
        if *name == "id" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
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
    let _ = writeln!(out, "(1) \"{}\".\"{}\"", keyspace, MV_BY_EMITEN_ID);
    let _ = writeln!(
        out,
        "SELECT \"emiten_id\", \"id\" (hanya kolom primary key MV)."
    );
    let _ = writeln!(
        out,
        "WHERE \"emiten_id\" IS NOT NULL AND \"id\" IS NOT NULL."
    );
    let _ = writeln!(
        out,
        "PRIMARY KEY: partition \"emiten_id\" (uuid); clustering \"id\" (uuid)."
    );
    let _ = writeln!(out, "WITH CLUSTERING ORDER BY (\"id\" ASC).");
    let _ = writeln!(
        out,
        "Gunakan: daftar bandarmology per emiten_id (contoh WHERE emiten_id = ?); data lengkap lewat tabel dasar WHERE id = ?."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "(2) \"{}\".\"{}\"", keyspace, MV_BY_AGG);
    let _ = writeln!(
        out,
        "SELECT \"agg_tahun_bulan_tanggal_emiten_name\", \"id\" (hanya kolom primary key MV)."
    );
    let _ = writeln!(
        out,
        "WHERE \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL AND \"id\" IS NOT NULL."
    );
    let _ = writeln!(
        out,
        "PRIMARY KEY: partition \"agg_tahun_bulan_tanggal_emiten_name\" (text); clustering \"id\" (uuid)."
    );
    let _ = writeln!(out, "WITH CLUSTERING ORDER BY (\"id\" ASC).");
    let _ = writeln!(
        out,
        "Catatan Scylla: MV hanya boleh punya satu kolom non-PK di primary key; \
         emiten_id tidak bisa clustering karena agg sudah memakai slot itu — gunakan id (PK tabel dasar)."
    );
    let _ = writeln!(
        out,
        "Kolom \"agg_tahun_bulan_tanggal_emiten_name\": text — diisi aplikasi sebagai \
         concat(tahun_bulan_tanggal, '_', emiten_name), contoh \"2026-07-16_BBCA\"."
    );
    let _ = writeln!(
        out,
        "Gunakan: lookup per agg (contoh WHERE agg_tahun_bulan_tanggal_emiten_name = ?); \
         data lengkap lewat tabel dasar WHERE id = ?."
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
            bandarmology_cql_output_path().display(), e
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

    for mv in [MV_BY_EMITEN_ID, MV_BY_AGG] {
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

    let ddl_mv_emiten = ddl_create_mv_by_emiten_id(&keyspace);
    session.query_unpaged(ddl_mv_emiten.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_EMITEN_ID}");

    let ddl_mv_agg = ddl_create_mv_by_agg(&keyspace);
    session.query_unpaged(ddl_mv_agg.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_AGG}");

    eprintln_bandarmology_schema_summary(&keyspace);

    Ok(())
}
