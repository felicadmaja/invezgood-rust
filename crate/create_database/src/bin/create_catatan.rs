//! Membuat tabel ScyllaDB `catatan`.
//!
//! ```bash
//! cargo run -p create_database --bin create_catatan
//! ```
//!
//! Kolom:
//! - `agg_tahun_bulan_tanggal_emiten_name` text — partition key;
//!   diisi aplikasi: `concat(tahun_bulan_tanggal, '_', emiten_name)` (contoh `2026-07-17_BBCA`)
//! - `tahun_bulan_tanggal` date
//! - `emiten_name` text
//! - `catatan` text
//!
//! Aman dijalankan ulang karena menggunakan `CREATE ... IF NOT EXISTS`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/catatan/src/catatan.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "catatan";

const CATATAN_COLUMNS: &[&str] = &[
    "agg_tahun_bulan_tanggal_emiten_name",
    "tahun_bulan_tanggal",
    "emiten_name",
    "catatan",
];

fn catatan_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../catatan/src/catatan.cql")
}

fn catatan_scylla_type(col: &str) -> &'static str {
    match col {
        "tahun_bulan_tanggal" => "date",
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
            \"agg_tahun_bulan_tanggal_emiten_name\" text, \
            \"tahun_bulan_tanggal\" date, \
            \"emiten_name\" text, \
            \"catatan\" text, \
            PRIMARY KEY ((\"agg_tahun_bulan_tanggal_emiten_name\"))\
        )"
    )
}

fn format_catatan_schema_summary(keyspace: &str) -> String {
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
    for name in CATATAN_COLUMNS {
        let ty = catatan_scylla_type(name);
        if *name == "agg_tahun_bulan_tanggal_emiten_name" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(
        out,
        "Kolom \"agg_tahun_bulan_tanggal_emiten_name\": text — diisi aplikasi sebagai \
         concat(tahun_bulan_tanggal, '_', emiten_name), contoh \"2026-07-17_BBCA\"."
    );
    let _ = writeln!(
        out,
        "Kolom \"tahun_bulan_tanggal\": date — tanggal catatan."
    );
    let _ = writeln!(
        out,
        "Kolom \"emiten_name\": text — kode emiten (contoh BBCA)."
    );
    let _ = writeln!(out, "Kolom \"catatan\": text — isi catatan.");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Gunakan: lookup catatan per agg (contoh WHERE agg_tahun_bulan_tanggal_emiten_name = ?)."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_catatan_schema_summary(keyspace: &str) {
    let text = format_catatan_schema_summary(keyspace);
    eprint!("\n{text}");
    let path = catatan_cql_output_path();
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

    eprintln_catatan_schema_summary(&keyspace);

    Ok(())
}
