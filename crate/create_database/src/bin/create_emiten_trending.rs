//! ```bash
//! cargo run -p create_database --bin create_emiten_trending
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`), tabel `emiten_trending`,
//! MV `emiten_trending_by_emiten_name`,
//! dan MV `emiten_trending_by_tahun_bulan_tanggal`.
//! Re-run: DROP MV + DROP TABLE lalu buat ulang (data emiten_trending hilang).
//!
//! Kolom tabel (tanpa uuid):
//! - `agg_tahun_bulan_tanggal_emiten_name` text — partition key; diisi aplikasi:
//!   `concat(tahun_bulan_tanggal, '_', emiten_name)` (contoh `2026-07-16_BBCA`)
//! - `tahun_bulan_tanggal` date
//! - `gainer_or_loser` text
//! - `emiten_name` text
//! - `price` double
//! - `price_change` double
//! - `value` text
//! - `volume` text
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/emiten_trending/src/emiten_trending.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "emiten_trending";
const MV_BY_EMITEN_NAME: &str = "emiten_trending_by_emiten_name";
const MV_BY_TAHUN_BULAN_TANGGAL: &str = "emiten_trending_by_tahun_bulan_tanggal";
/// MV lama (uuid-era) yang di-drop pada re-run.
const LEGACY_MV_BY_AGG: &str = "emiten_trending_by_agg_tahun_bulan_tanggal_emiten_name";

const EMITEN_TRENDING_COLUMNS: &[&str] = &[
    "agg_tahun_bulan_tanggal_emiten_name",
    "tahun_bulan_tanggal",
    "gainer_or_loser",
    "emiten_name",
    "price",
    "price_change",
    "value",
    "volume",
];

fn emiten_trending_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../emiten_trending/src/emiten_trending.cql")
}

fn emiten_trending_scylla_type(col: &str) -> &'static str {
    match col {
        "tahun_bulan_tanggal" => "date",
        "price" | "price_change" => "double",
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
            \"agg_tahun_bulan_tanggal_emiten_name\" text, \
            \"tahun_bulan_tanggal\" date, \
            \"gainer_or_loser\" text, \
            \"emiten_name\" text, \
            \"price\" double, \
            \"price_change\" double, \
            \"value\" text, \
            \"volume\" text, \
            PRIMARY KEY ((\"agg_tahun_bulan_tanggal_emiten_name\"))\
        )",
        keyspace, TABLE
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

fn ddl_create_mv_by_tahun_bulan_tanggal(keyspace: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS \
         SELECT \"tahun_bulan_tanggal\", \"agg_tahun_bulan_tanggal_emiten_name\" FROM {}.{} \
         WHERE \"tahun_bulan_tanggal\" IS NOT NULL AND \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL \
         PRIMARY KEY ((\"tahun_bulan_tanggal\"), \"agg_tahun_bulan_tanggal_emiten_name\") \
         WITH CLUSTERING ORDER BY (\"agg_tahun_bulan_tanggal_emiten_name\" ASC)",
        keyspace, MV_BY_TAHUN_BULAN_TANGGAL, keyspace, TABLE
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

fn format_emiten_trending_schema_summary(keyspace: &str) -> String {
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
    for name in EMITEN_TRENDING_COLUMNS {
        let ty = emiten_trending_scylla_type(name);
        if *name == "agg_tahun_bulan_tanggal_emiten_name" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(
        out,
        "Kolom \"agg_tahun_bulan_tanggal_emiten_name\": text — diisi aplikasi sebagai \
         concat(tahun_bulan_tanggal, '_', emiten_name), contoh \"2026-07-16_BBCA\"."
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
        "Gunakan: daftar trending per emiten_name (contoh WHERE emiten_name = ?); \
         data lengkap lewat tabel dasar WHERE agg_tahun_bulan_tanggal_emiten_name = ?."
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "(2) \"{}\".\"{}\"",
        keyspace, MV_BY_TAHUN_BULAN_TANGGAL
    );
    let _ = writeln!(
        out,
        "SELECT \"tahun_bulan_tanggal\", \"agg_tahun_bulan_tanggal_emiten_name\" (hanya kolom primary key MV)."
    );
    let _ = writeln!(
        out,
        "WHERE \"tahun_bulan_tanggal\" IS NOT NULL AND \"agg_tahun_bulan_tanggal_emiten_name\" IS NOT NULL."
    );
    let _ = writeln!(
        out,
        "PRIMARY KEY: partition \"tahun_bulan_tanggal\" (date); clustering \"agg_tahun_bulan_tanggal_emiten_name\" (text)."
    );
    let _ = writeln!(
        out,
        "WITH CLUSTERING ORDER BY (\"agg_tahun_bulan_tanggal_emiten_name\" ASC)."
    );
    let _ = writeln!(
        out,
        "Gunakan: daftar trending per tanggal (contoh WHERE tahun_bulan_tanggal = ?); \
         data lengkap lewat tabel dasar WHERE agg_tahun_bulan_tanggal_emiten_name = ?."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

fn eprintln_emiten_trending_schema_summary(keyspace: &str) {
    let text = format_emiten_trending_schema_summary(keyspace);
    eprint!("\n{text}");
    if let Some(parent) = emiten_trending_cql_output_path().as_path().parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "Peringatan: gagal membuat direktori {}: {}",
                parent.display(),
                e
            );
        }
    }
    match std::fs::write(emiten_trending_cql_output_path(), &text) {
        Ok(()) => eprintln!(
            "OK: ringkasan skema ditulis ke {}",
            emiten_trending_cql_output_path().display()
        ),
        Err(e) => eprintln!(
            "Peringatan: gagal menulis {}: {}",
            emiten_trending_cql_output_path().display(),
            e
        ),
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

    for mv in [
        LEGACY_MV_BY_AGG,
        MV_BY_EMITEN_NAME,
        MV_BY_TAHUN_BULAN_TANGGAL,
    ] {
        let ddl_drop_mv = ddl_drop_materialized_view(&keyspace, mv);
        session.query_unpaged(ddl_drop_mv.as_str(), &[]).await?;
        eprintln!("OK: {ddl_drop_mv}");
    }

    let ddl_drop_table = ddl_drop_table(&keyspace);
    session.query_unpaged(ddl_drop_table.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_table}");

    let ddl_table = ddl_create_table(&keyspace);
    session.query_unpaged(ddl_table.as_str(), &[]).await?;
    eprintln!("OK: CREATE TABLE {keyspace}.{TABLE}");

    let ddl_mv_name = ddl_create_mv_by_emiten_name(&keyspace);
    session.query_unpaged(ddl_mv_name.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_EMITEN_NAME}");

    let ddl_mv_date = ddl_create_mv_by_tahun_bulan_tanggal(&keyspace);
    session.query_unpaged(ddl_mv_date.as_str(), &[]).await?;
    eprintln!("OK: CREATE MATERIALIZED VIEW IF NOT EXISTS {keyspace}.{MV_BY_TAHUN_BULAN_TANGGAL}");

    eprintln_emiten_trending_schema_summary(&keyspace);

    Ok(())
}
