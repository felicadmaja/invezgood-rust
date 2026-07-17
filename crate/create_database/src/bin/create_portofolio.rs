//! ```bash
//! cargo run -p create_database --bin create_portofolio
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`), tabel `portofolio`,
//! dan MV `portofolio_by_emiten_name`.
//! Re-run: DROP MV + DROP TABLE lalu buat ulang (data portofolio hilang).
//!
//! Kolom tabel:
//! - `id` uuid — partition key
//! - `emiten_name` text
//! - `balance_lot` bigint
//! - `available_lot` bigint
//! - `average_price` double
//! - `current_price` double
//! - `invested` double
//! - `market_value` double
//! - `potential_p_l` double
//! - `percentage` double
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/portofolio/src/portofolio.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "portofolio";
const MV_BY_EMITEN_NAME: &str = "portofolio_by_emiten_name";

const PORTOFOLIO_COLUMNS: &[&str] = &[
    "id",
    "emiten_name",
    "balance_lot",
    "available_lot",
    "average_price",
    "current_price",
    "invested",
    "market_value",
    "potential_p_l",
    "percentage",
];

fn portofolio_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../portofolio/src/portofolio.cql")
}

fn portofolio_scylla_type(col: &str) -> &'static str {
    match col {
        "id" => "uuid",
        "balance_lot" | "available_lot" => "bigint",
        "average_price"
        | "current_price"
        | "invested"
        | "market_value"
        | "potential_p_l"
        | "percentage" => "double",
        _ => "text",
    }
}

fn ddl_create_keyspace(keyspace: &str) -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        keyspace
    )
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
            \"id\" uuid, \
            \"emiten_name\" text, \
            \"balance_lot\" bigint, \
            \"available_lot\" bigint, \
            \"average_price\" double, \
            \"current_price\" double, \
            \"invested\" double, \
            \"market_value\" double, \
            \"potential_p_l\" double, \
            \"percentage\" double, \
            PRIMARY KEY ((\"id\"))\
        )",
        keyspace, TABLE
    )
}

fn ddl_create_mv_by_emiten_name(keyspace: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS \
         SELECT \"emiten_name\", \"id\" FROM {}.{} \
         WHERE \"emiten_name\" IS NOT NULL AND \"id\" IS NOT NULL \
         PRIMARY KEY ((\"emiten_name\"), \"id\") \
         WITH CLUSTERING ORDER BY (\"id\" ASC)",
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

fn format_portofolio_schema_summary(keyspace: &str) -> String {
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
    for name in PORTOFOLIO_COLUMNS {
        let ty = portofolio_scylla_type(name);
        if *name == "id" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Materialized view (MV) ---");
    let _ = writeln!(out, "(1) \"{}\".\"{}\"", keyspace, MV_BY_EMITEN_NAME);
    let _ = writeln!(
        out,
        "SELECT \"emiten_name\", \"id\" (hanya kolom primary key MV)."
    );
    let _ = writeln!(
        out,
        "WHERE \"emiten_name\" IS NOT NULL AND \"id\" IS NOT NULL."
    );
    let _ = writeln!(
        out,
        "PRIMARY KEY: partition \"emiten_name\" (text); clustering \"id\" (uuid)."
    );
    let _ = writeln!(out, "WITH CLUSTERING ORDER BY (\"id\" ASC).");
    let _ = writeln!(
        out,
        "Gunakan: daftar portofolio per emiten_name (contoh WHERE emiten_name = ?); \
         data lengkap lewat tabel dasar WHERE id = ?."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_portofolio_schema_summary(keyspace: &str) {
    let text = format_portofolio_schema_summary(keyspace);
    eprint!("\n{text}");
    if let Some(parent) = portofolio_cql_output_path().as_path().parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "Peringatan: gagal membuat direktori {}: {}",
                parent.display(),
                e
            );
        }
    }
    match std::fs::write(portofolio_cql_output_path(), &text) {
        Ok(()) => eprintln!(
            "OK: ringkasan skema ditulis ke {}",
            portofolio_cql_output_path().display()
        ),
        Err(e) => eprintln!(
            "Peringatan: gagal menulis {}: {}",
            portofolio_cql_output_path().display(), e
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

    let ddl_drop_mv = ddl_drop_materialized_view(&keyspace, MV_BY_EMITEN_NAME);
    session.query_unpaged(ddl_drop_mv.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_mv}");

    let ddl_drop_table = ddl_drop_table(&keyspace);
    session.query_unpaged(ddl_drop_table.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_table}");

    let ddl_table = ddl_create_table(&keyspace);
    session.query_unpaged(ddl_table.as_str(), &[]).await?;
    eprintln!("OK: CREATE TABLE {keyspace}.{TABLE}");

    let ddl_mv = ddl_create_mv_by_emiten_name(&keyspace);
    session.query_unpaged(ddl_mv.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_EMITEN_NAME}");

    eprintln_portofolio_schema_summary(&keyspace);

    Ok(())
}
