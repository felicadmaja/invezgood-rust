//! ```bash
//! cargo run -p create_database --bin create_broker_name
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`) dan tabel `broker`.
//! Re-run: DROP TABLE lalu buat ulang (data broker hilang).
//!
//! Kolom tabel:
//! - `broker_code` text — partition key
//! - `name` text
//! - `tipe` text
//! - `asosiasi` text
//! - `catatan` text
//!
//! Migrasi: DROP MV/tabel legacy `broker_name` + `broker_name_by_code_name` bila masih ada.
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/broker/src/broker.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "broker";
const LEGACY_TABLE: &str = "broker_name";
const MV_LEGACY_BY_CODE_NAME: &str = "broker_name_by_code_name";

const BROKER_COLUMNS: &[&str] = &["broker_code", "name", "tipe", "asosiasi", "catatan"];

fn broker_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../broker/src/broker.cql")
}

fn ddl_create_keyspace(keyspace: &str) -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        keyspace
    )
}

fn ddl_drop_table(keyspace: &str, table: &str) -> String {
    format!("DROP TABLE IF EXISTS {}.{}", keyspace, table)
}

fn ddl_drop_materialized_view(keyspace: &str, mv: &str) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}.{}", keyspace, mv)
}

fn ddl_create_table(keyspace: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\
            \"broker_code\" text, \
            \"name\" text, \
            \"tipe\" text, \
            \"asosiasi\" text, \
            \"catatan\" text, \
            PRIMARY KEY ((\"broker_code\"))\
        )",
        keyspace, TABLE
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

fn format_broker_schema_summary(keyspace: &str) -> String {
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
        "Primary key: ((\"broker_code\")) — \"broker_code\" text."
    );
    let _ = writeln!(out, "Kolom dan tipe CQL:");
    for name in BROKER_COLUMNS {
        if *name == "broker_code" {
            let _ = writeln!(out, "  \"{}\" text — partition key", name);
        } else {
            let _ = writeln!(out, "  \"{}\" text", name);
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Lookup: WHERE broker_code = ? (contoh kode broker IDX)."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Materialized view (MV) ---");
    let _ = writeln!(
        out,
        "Tidak ada: MV \"{}\".\"{}\" (legacy) di-drop saat migrasi; \
         tabel legacy \"{}\".\"{}\" juga di-drop.",
        keyspace, MV_LEGACY_BY_CODE_NAME, keyspace, LEGACY_TABLE
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Secondary index ---");
    let _ = writeln!(
        out,
        "Tidak ada: akses lewat PRIMARY KEY ((\"broker_code\"))."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_broker_schema_summary(keyspace: &str) {
    let text = format_broker_schema_summary(keyspace);
    eprint!("\n{text}");
    let path = broker_cql_output_path();
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "Peringatan: gagal membuat direktori {}: {}",
                parent.display(),
                e
            );
        }
    }
    match std::fs::write(&path, &text) {
        Ok(()) => eprintln!("OK: ringkasan skema ditulis ke {}", path.display()),
        Err(e) => eprintln!("Peringatan: gagal menulis {}: {}", path.display(), e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    load_dotenv();

    let keyspace =
        std::env::var("SCYLLA_KEYSPACE").map_err(|_| "SCYLLA_KEYSPACE wajib diisi di .env")?;

    let session = connect_session().await?;

    let ddl_keyspace = ddl_create_keyspace(&keyspace);
    session.query_unpaged(ddl_keyspace.as_str(), &[]).await?;
    eprintln!("OK: CREATE KEYSPACE IF NOT EXISTS {keyspace}");

    let ddl_drop_mv = ddl_drop_materialized_view(&keyspace, MV_LEGACY_BY_CODE_NAME);
    session.query_unpaged(ddl_drop_mv.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_mv}");

    let ddl_drop_legacy = ddl_drop_table(&keyspace, LEGACY_TABLE);
    session.query_unpaged(ddl_drop_legacy.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_legacy}");

    let ddl_drop = ddl_drop_table(&keyspace, TABLE);
    session.query_unpaged(ddl_drop.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop}");

    let ddl_table = ddl_create_table(&keyspace);
    session.query_unpaged(ddl_table.as_str(), &[]).await?;
    eprintln!("OK: CREATE TABLE {keyspace}.{TABLE}");

    eprintln_broker_schema_summary(&keyspace);

    Ok(())
}
