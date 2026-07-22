//! ```bash
//! cargo run -p create_database --bin backfill_emiten_trending_sector
//! ```
//! Salin `emiten_list.sector` (tinyint) → `emiten_trending.sector` (tinyint, mapping sama).

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::DeserializeRow;
use std::sync::Arc;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;

#[derive(Debug, DeserializeRow)]
struct ListSectorRow {
    #[scylla(default_when_null)]
    emiten_name: String,
    sector: Option<i8>,
}

#[derive(Debug, DeserializeRow)]
struct TrendingAggRow {
    #[scylla(default_when_null)]
    agg_tahun_bulan_tanggal_emiten_name: String,
}

fn load_dotenv() {
    let workspace_env =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
        return;
    }
    dotenvy::dotenv().ok();
}

async fn connect_session() -> Result<Arc<Session>, Box<dyn std::error::Error + Send + Sync>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri.as_str());
    if let Ok(user) = std::env::var("SCYLLA_USER") {
        if let Ok(password) = std::env::var("SCYLLA_PASSWORD") {
            builder = builder.user(user, password);
        }
    }
    Ok(Arc::new(builder.build().await?))
}

fn token_segment_start(seg: usize, num_seg: usize) -> i64 {
    if seg == 0 {
        i64::MIN
    } else {
        let span = (i64::MAX as i128) - (i64::MIN as i128);
        (i64::MIN as i128 + (span * seg as i128) / num_seg as i128) as i64
    }
}

fn token_segment_end(seg: usize, num_seg: usize) -> i64 {
    if seg + 1 == num_seg {
        i64::MAX
    } else {
        token_segment_start(seg + 1, num_seg).saturating_sub(1)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    load_dotenv();
    let ks = std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string());
    let session = connect_session().await?;

    let mut scan = session
        .prepare(format!(
            "SELECT emiten_name, sector FROM {ks}.emiten_list \
             WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?"
        ))
        .await?;
    scan.set_page_size(200);

    let mv_by_emiten = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name \
             FROM {ks}.emiten_trending_by_emiten_name WHERE emiten_name = ?"
        ))
        .await?;
    let update_sector = session
        .prepare(format!(
            "UPDATE {ks}.emiten_trending SET sector = ? \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;

    let segment_rows: Vec<Vec<ListSectorRow>> = stream::iter(0..TOKEN_SEGMENTS)
        .map(|seg| {
            let session = Arc::clone(&session);
            let stmt = scan.clone();
            let start = token_segment_start(seg, TOKEN_SEGMENTS);
            let end = token_segment_end(seg, TOKEN_SEGMENTS);
            async move {
                let pager = session.execute_iter(stmt, (start, end)).await?;
                let mut rows = pager.rows_stream::<ListSectorRow>()?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await {
                    out.push(row?);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(out)
            }
        })
        .buffer_unordered(SCAN_CONCURRENCY)
        .try_collect()
        .await?;

    let list_rows: Vec<ListSectorRow> = segment_rows.into_iter().flatten().collect();
    println!("emiten_list rows scanned: {}", list_rows.len());

    let mut updated = 0usize;
    let mut skipped_empty = 0usize;
    let mut emitens_with_sector = 0usize;

    for row in list_rows {
        let code = row.emiten_name.trim().to_ascii_uppercase();
        if code.is_empty() {
            continue;
        }
        let Some(sector) = row.sector.filter(|&s| s > 0) else {
            skipped_empty += 1;
            continue;
        };
        emitens_with_sector += 1;

        let mv = session
            .execute_unpaged(&mv_by_emiten, (code.as_str(),))
            .await?
            .into_rows_result()?;
        for tr in mv.rows::<TrendingAggRow>()? {
            let agg = tr?.agg_tahun_bulan_tanggal_emiten_name;
            if agg.is_empty() {
                continue;
            }
            session
                .execute_unpaged(&update_sector, (sector, agg.as_str()))
                .await?;
            updated += 1;
        }
    }

    println!(
        "OK: emitens_with_sector={emitens_with_sector} skipped_empty_sector={skipped_empty} \
         emiten_trending rows updated={updated}"
    );
    Ok(())
}
