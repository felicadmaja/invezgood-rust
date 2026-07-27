//! Orphan cleanup `portofolio_bandarmology` (RPC `DeletePortofolioBandarmology`).
//!
//! Tidak ada insert/update ke `portofolio_bandarmology` lagi — data tersebut tidak diisi
//! oleh worker / on-demand scrape.
//!
//! Cleanup: token-scan `portofolio_bandarmology` → hapus partition bila emiten
//! **tidak** ada di `portofolio`.

use std::collections::HashSet;
use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::DeserializeRow;

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

#[derive(Debug, DeserializeRow)]
struct EmitenNameOnly {
    #[scylla(default_when_null)]
    emiten_name: String,
}

#[derive(Debug, DeserializeRow)]
#[allow(dead_code)]
struct PortofolioExistsRow {
    #[scylla(default_when_null)]
    emiten_name: String,
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

async fn portofolio_exists(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let stmt = session
        .prepare(format!(
            "SELECT emiten_name FROM {keyspace}.portofolio WHERE emiten_name = ? LIMIT 1"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (emiten_name,))
        .await?
        .into_rows_result()?;
    Ok(result.maybe_first_row::<PortofolioExistsRow>()?.is_some())
}

/// Hapus partition `portofolio_bandarmology` yang emiten-nya sudah tidak ada di `portofolio`.
/// Sama alur RPC `DeletePortofolioBandarmology`. Returns jumlah emiten yang dihapus.
pub async fn delete_unused_portofolio_bandarmology(
    session: &Arc<Session>,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut scan = session
        .prepare(format!(
            "SELECT emiten_name FROM {keyspace}.portofolio_bandarmology \
             WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?"
        ))
        .await?;
    scan.set_page_size(PAGE_SIZE);

    let segment_sets: Vec<HashSet<String>> = stream::iter(0..TOKEN_SEGMENTS)
        .map(|seg| {
            let session = Arc::clone(session);
            let stmt = scan.clone();
            let start = token_segment_start(seg, TOKEN_SEGMENTS);
            let end = token_segment_end(seg, TOKEN_SEGMENTS);
            async move {
                let pager = session.execute_iter(stmt, (start, end)).await?;
                let mut rows = pager.rows_stream::<EmitenNameOnly>()?;
                let mut out = HashSet::new();
                while let Some(row) = rows.next().await {
                    let name = row?.emiten_name.trim().to_ascii_uppercase();
                    if !name.is_empty() {
                        out.insert(name);
                    }
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(out)
            }
        })
        .buffer_unordered(SCAN_CONCURRENCY)
        .try_collect()
        .await?;

    let names: HashSet<String> = segment_sets.into_iter().flatten().collect();
    let delete = session
        .prepare(format!(
            "DELETE FROM {keyspace}.portofolio_bandarmology WHERE emiten_name = ?"
        ))
        .await?;

    let mut deleted = 0usize;
    for name in names {
        if portofolio_exists(session.as_ref(), keyspace, &name).await? {
            continue;
        }
        session
            .execute_unpaged(&delete, (name.as_str(),))
            .await?;
        deleted += 1;
        println!(
            "portofolio_bandarmology: hapus orphan {name} (tidak ada di portofolio)"
        );
    }
    Ok(deleted)
}
