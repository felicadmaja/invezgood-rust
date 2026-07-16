//! ```bash
//! cargo run -p create_database --bin create_emiten_list
//! ```
//! Buat keyspace `stockbit` (dari env `SCYLLA_KEYSPACE`) dan tabel `emiten_list`.
//! Re-run: DROP legacy MV (jika ada) + DROP TABLE + DROP TYPE lalu buat ulang (data emiten_list hilang).
//!
//! Kolom tabel:
//! - `code_name` text — partition key
//! - `long_name` text
//! - `key_stats` map<text, text>
//! - `income_statement_ttm` map<text, frozen<map<text, text>>> — pos laporan laba rugi TTM per kuartal
//! - `balance_sheet_quarterly` map<text, frozen<map<text, text>>> — pos neraca kuartalan per periode
//! - `cash_flow_ttm` map<text, frozen<map<text, text>>> — pos arus kas TTM per kuartal
//! - `corporate_action` map<text, frozen<map<text, text>>> — aksi korporasi emiten
//! - `shareholder` list<text>
//! - `company_profile` frozen<company_profile> — profil perusahaan (UDT)
//! - `update_at` timestamp — waktu terakhir data diperbarui
//!
//! Env: `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Di akhir sukses: ringkasan skema ke stderr dan ke **`crate/emiten_list/src/emiten_list.cql`**.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

const TABLE: &str = "emiten_list";
const LEGACY_MV_BY_NAME: &str = "emiten_list_by_name";
const UDT_SHAREHOLDER_GT1: &str = "emiten_shareholder_gt1";
const UDT_SHAREHOLDER: &str = "emiten_shareholder";
const UDT_COMPANY_PROFILE: &str = "company_profile";

const EMITEN_LIST_COLUMNS: &[&str] = &[
    "code_name",
    "long_name",
    "key_stats",
    "income_statement_ttm",
    "balance_sheet_quarterly",
    "cash_flow_ttm",
    "corporate_action",
    "shareholder",
    "company_profile",
    "update_at",
];

fn emiten_list_cql_output_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../emiten_list/src/emiten_list.cql")
}

fn emiten_scylla_type(col: &str) -> &'static str {
    match col {
        "shareholder" => "list<text>",
        "key_stats" => "map<text, text>",
        "income_statement_ttm"
        | "balance_sheet_quarterly"
        | "cash_flow_ttm"
        | "corporate_action" => "map<text, frozen<map<text, text>>>",
        "company_profile" => "frozen<company_profile>",
        "update_at" => "timestamp",
        _ => "text",
    }
}

fn ddl_create_keyspace(keyspace: &str) -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        keyspace
    )
}

fn ddl_create_udt_shareholder_gt1(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            name text, \
            type text, \
            location text, \
            domicile text, \
            scriples text, \
            scrip text, \
            total_shares text, \
            percentage text\
        )",
        keyspace, UDT_SHAREHOLDER_GT1
    )
}

fn ddl_create_udt_shareholder(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            name text, \
            value text, \
            shares text\
        )",
        keyspace, UDT_SHAREHOLDER
    )
}

fn ddl_create_udt_company_profile(keyspace: &str) -> String {
    format!(
        "CREATE TYPE IF NOT EXISTS {}.{} (\
            company_background text, \
            sector text, \
            shareholder_more_than_one_percent list<frozen<{}>>, \
            shareholders list<frozen<{}>>, \
            ultimate_beneficial_owner text\
        )",
        keyspace, UDT_COMPANY_PROFILE, UDT_SHAREHOLDER_GT1, UDT_SHAREHOLDER
    )
}

fn ddl_drop_udt(keyspace: &str, type_name: &str) -> String {
    format!("DROP TYPE IF EXISTS {}.{}", keyspace, type_name)
}

fn ddl_create_table(keyspace: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\
            \"code_name\" text, \
            \"long_name\" text, \
            \"key_stats\" map<text, text>, \
            \"income_statement_ttm\" map<text, frozen<map<text, text>>>, \
            \"balance_sheet_quarterly\" map<text, frozen<map<text, text>>>, \
            \"cash_flow_ttm\" map<text, frozen<map<text, text>>>, \
            \"corporate_action\" map<text, frozen<map<text, text>>>, \
            \"shareholder\" list<text>, \
            \"company_profile\" frozen<{}>, \
            \"update_at\" timestamp, \
            PRIMARY KEY ((\"code_name\"))\
        )",
        keyspace, TABLE, UDT_COMPANY_PROFILE
    )
}

fn ddl_drop_table(keyspace: &str) -> String {
    format!("DROP TABLE IF EXISTS {}.{}", keyspace, TABLE)
}

fn ddl_drop_legacy_materialized_view(keyspace: &str) -> String {
    format!(
        "DROP MATERIALIZED VIEW IF EXISTS {}.{}",
        keyspace, LEGACY_MV_BY_NAME
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

/// Ringkasan struktur (base table, MV, indeks sekunder, UDT, enum).
fn format_emiten_list_schema_summary(keyspace: &str) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "=== Struktur migrasi {}.{} (schema summary, kata demi kata) ===",
        keyspace, TABLE
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Keyspace: \"{}\"", keyspace);
    let _ = writeln!(
        out,
        "Replikasi (saat CREATE KEYSPACE): class SimpleStrategy, replication factor 1."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Tabel dasar (base table) ---");
    let _ = writeln!(out, "Nama penuh: \"{}\".\"{}\"", keyspace, TABLE);
    let _ = writeln!(
        out,
        "Primary key: ((\"code_name\")) — \"code_name\" text."
    );
    let _ = writeln!(out, "Tidak ada clustering key.");
    let _ = writeln!(out, "Kolom dan tipe CQL, kata demi kata:");
    for name in EMITEN_LIST_COLUMNS {
        let ty = emiten_scylla_type(name);
        if *name == "code_name" {
            let _ = writeln!(out, "  \"{}\" {} — partition key", name, ty);
        } else {
            let _ = writeln!(out, "  \"{}\" {}", name, ty);
        }
    }
    let _ = writeln!(
        out,
        "Sumber: DDL bin create_emiten_list; diisi aplikasi pada INSERT."
    );
    let _ = writeln!(
        out,
        "Kolom \"code_name\": text — kode/nama emiten (contoh BBCA, BBRI)."
    );
    let _ = writeln!(
        out,
        "Kolom \"long_name\": text — nama lengkap emiten (contoh Bank Central Asia Tbk)."
    );
    let _ = writeln!(
        out,
        "Kolom \"key_stats\": map<text, text> — statistik kunci emiten (pasangan key-value)."
    );
    let _ = writeln!(
        out,
        "Kolom \"income_statement_ttm\": map<text, frozen<map<text, text>>> — pos laporan laba rugi TTM; \
         key luar = nama pos (contoh \"Total Pendapatan\"), key dalam = periode kuartal (contoh \"Q1 2026\"), \
         value = nilai terformat (contoh \"3,890 B\")."
    );
    let _ = writeln!(
        out,
        "Kolom \"balance_sheet_quarterly\": map<text, frozen<map<text, text>>> — pos neraca kuartalan; \
         key luar = nama pos (contoh \"Total Aset\"), key dalam = periode kuartal (contoh \"Q1 2026\"), \
         value = nilai terformat (contoh \"3,890 B\")."
    );
    let _ = writeln!(
        out,
        "Kolom \"cash_flow_ttm\": map<text, frozen<map<text, text>>> — pos arus kas TTM; \
         key luar = nama pos (contoh \"Arus Kas Operasi\"), key dalam = periode kuartal (contoh \"Q1 2026\"), \
         value = nilai terformat (contoh \"3,890 B\")."
    );
    let _ = writeln!(
        out,
        "Kolom \"corporate_action\": map<text, frozen<map<text, text>>> — aksi korporasi; \
         key luar = jenis/tanggal aksi (contoh \"Dividen 2026-03-15\"), key dalam = atribut (contoh \"ex_date\", \"ratio\"), \
         value = nilai terformat."
    );
    let _ = writeln!(
        out,
        "Kolom \"shareholder\": list<text> — daftar pemegang saham (ringkas)."
    );
    let _ = writeln!(
        out,
        "Kolom \"company_profile\": frozen<{}> — profil perusahaan (lihat UDT di bawah).",
        UDT_COMPANY_PROFILE
    );
    let _ = writeln!(
        out,
        "Kolom \"update_at\": timestamp — waktu terakhir data emiten diperbarui."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Materialized view (MV) ---");
    let _ = writeln!(
        out,
        "Tidak ada: MV \"{}\".\"{}\" (legacy) di-drop saat migrasi; lookup langsung lewat PRIMARY KEY ((\"code_name\")).",
        keyspace, LEGACY_MV_BY_NAME
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Secondary index ---");
    let _ = writeln!(
        out,
        "Tidak ada: tidak dibuat CREATE INDEX / SAI pada \"{}\".\"{}\"; akses lewat PRIMARY KEY ((\"code_name\")).",
        keyspace, TABLE
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- User-defined type (UDT) ---");
    let _ = writeln!(
        out,
        "(1) \"{}\".\"{}\" — pemegang saham >1%: name, type, location, domicile, scriples, scrip, total_shares, percentage (semua text).",
        keyspace, UDT_SHAREHOLDER_GT1
    );
    let _ = writeln!(
        out,
        "(2) \"{}\".\"{}\" — ringkasan pemegang saham: name, value, shares (semua text).",
        keyspace, UDT_SHAREHOLDER
    );
    let _ = writeln!(
        out,
        "(3) \"{}\".\"{}\" — company_background text; sector text; \
         shareholder_more_than_one_percent list<frozen<{}>>; \
         shareholders list<frozen<{}>>; \
         ultimate_beneficial_owner text.",
        keyspace, UDT_COMPANY_PROFILE, UDT_SHAREHOLDER_GT1, UDT_SHAREHOLDER
    );
    let _ = writeln!(
        out,
        "Mapping dari sumber UI: \"Company Background\"→company_background, \"Sector\"→sector, \
         \"Shareholder_more_than_one_percent\"→shareholder_more_than_one_percent (list), \
         \"shareholders\"→shareholders, \"Ultimate Beneficial Owner\"→ultimate_beneficial_owner."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Enum (tipe enum CQL) ---");
    let _ = writeln!(
        out,
        "Tidak ada: tidak ada kolom bertipe enum CQL pada tabel emiten_list."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "=== Akhir ringkasan struktur ===");
    out
}

/// Cetak ringkasan skema ke stderr dan tulis ke [`emiten_list_cql_output_path()`].
fn eprintln_emiten_list_schema_summary(keyspace: &str) {
    let text = format_emiten_list_schema_summary(keyspace);
    eprint!("\n{text}");
    if let Some(parent) = emiten_list_cql_output_path().as_path().parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "Peringatan: gagal membuat direktori {}: {}",
                parent.display(),
                e
            );
        }
    }
    match std::fs::write(emiten_list_cql_output_path(), &text) {
        Ok(()) => eprintln!(
            "OK: ringkasan skema ditulis ke {}",
            emiten_list_cql_output_path().display()
        ),
        Err(e) => eprintln!(
            "Peringatan: gagal menulis {}: {}",
            emiten_list_cql_output_path().display(), e
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

    let ddl_drop_mv = ddl_drop_legacy_materialized_view(&keyspace);
    session.query_unpaged(ddl_drop_mv.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_mv}");

    let ddl_drop_table = ddl_drop_table(&keyspace);
    session.query_unpaged(ddl_drop_table.as_str(), &[]).await?;
    eprintln!("OK: {ddl_drop_table}");

    // Drop UDT dari yang bergantung ke yang independen.
    for type_name in [UDT_COMPANY_PROFILE, UDT_SHAREHOLDER, UDT_SHAREHOLDER_GT1] {
        let ddl = ddl_drop_udt(&keyspace, type_name);
        session.query_unpaged(ddl.as_str(), &[]).await?;
        eprintln!("OK: {ddl}");
    }

    let ddl_gt1 = ddl_create_udt_shareholder_gt1(&keyspace);
    session.query_unpaged(ddl_gt1.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_SHAREHOLDER_GT1}");

    let ddl_sh = ddl_create_udt_shareholder(&keyspace);
    session.query_unpaged(ddl_sh.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_SHAREHOLDER}");

    let ddl_cp = ddl_create_udt_company_profile(&keyspace);
    session.query_unpaged(ddl_cp.as_str(), &[]).await?;
    eprintln!("OK: CREATE TYPE {keyspace}.{UDT_COMPANY_PROFILE}");

    let ddl_table = ddl_create_table(&keyspace);
    session.query_unpaged(ddl_table.as_str(), &[]).await?;
    eprintln!("OK: CREATE TABLE {keyspace}.{TABLE}");

    eprintln_emiten_list_schema_summary(&keyspace);

    Ok(())
}
