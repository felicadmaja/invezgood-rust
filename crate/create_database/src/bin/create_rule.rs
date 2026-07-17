//! Membuat tabel ScyllaDB `rule`.
//!
//! ```bash
//! cargo run -p create_database --bin create_rule
//! ```
//!
//! Aman dijalankan ulang karena menggunakan `CREATE ... IF NOT EXISTS`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/rule/src/rule.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "rule";

const RULE_COLUMNS: &[&str] = &[
    "rule_name",
    "rule_description",
    "rule_parameter",
    "rule_gt_or_lt",
    "rule_value",
];

fn rule_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rule/src/rule.cql")
}

fn rule_scylla_type(col: &str) -> &'static str {
    match col {
        "rule_gt_or_lt" => "tinyint",
        "rule_value" => "double",
        _ => "text",
    }
}

fn load_dotenv() {
    use std::path::PathBuf;

    let workspace_env = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }
}

async fn connect_session() -> Result<Session, Box<dyn std::error::Error + Send + Sync>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri);

    if let (Ok(user), Ok(password)) = (
        std::env::var("SCYLLA_USER"),
        std::env::var("SCYLLA_PASSWORD"),
    ) {
        builder = builder.user(user, password);
    }

    Ok(builder.build().await?)
}

fn ddl_create_keyspace(keyspace: &str) -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {keyspace} \
         WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
}

fn ddl_create_table(keyspace: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {keyspace}.\"{TABLE}\" (\
            rule_name text PRIMARY KEY, \
            rule_description text, \
            rule_parameter text, \
            rule_gt_or_lt tinyint, \
            rule_value double\
        )"
    )
}

fn format_rule_schema_summary(keyspace: &str) -> String {
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
    let _ = writeln!(out, "Primary key: ((\"rule_name\")) — text.");
    let _ = writeln!(out, "Kolom dan tipe CQL:");
    for name in RULE_COLUMNS {
        let ty = rule_scylla_type(name);
        if *name == "rule_name" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Gunakan: lookup rule per rule_name (contoh WHERE rule_name = ?)."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_rule_schema_summary(keyspace: &str) {
    let text = format_rule_schema_summary(keyspace);
    eprint!("\n{text}");
    let path = rule_cql_output_path();
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

    let keyspace = std::env::var("SCYLLA_KEYSPACE")
        .map_err(|_| "SCYLLA_KEYSPACE wajib diisi di .env")?;
    let session = connect_session().await?;

    session
        .query_unpaged(ddl_create_keyspace(&keyspace), &[])
        .await?;
    eprintln!("OK: CREATE KEYSPACE IF NOT EXISTS {keyspace}");

    session
        .query_unpaged(ddl_create_table(&keyspace), &[])
        .await?;
    eprintln!("OK: CREATE TABLE IF NOT EXISTS {keyspace}.{TABLE}");

    eprintln_rule_schema_summary(&keyspace);

    Ok(())
}
