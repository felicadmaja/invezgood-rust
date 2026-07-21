//! Salin snapshot bandarmology minggu berjalan → `portofolio_bandarmology`.
//!
//! Alur sama RPC `InsertPortofolioBandarmology`:
//! - Baca `bandarmology` PK sesuai slot minggu (bulan berjalan, kecuali tgl 1 → bulan lalu)
//! - Pilih `broker_summary_current_w1`…`w4` menurut tanggal hari ini (lokal):
//!   2–8 → w1, 9–15 → w2, 16–22 → w3, 23–akhir bulan & tgl 1 → w4
//!   (PK bandarmology: bulan berjalan, kecuali tgl 1 → agg bulan sebelumnya — sama worker bandarmology)
//! - Upsert `portofolio_bandarmology` (emiten_name, today, day)
//!
//! Orphan cleanup sama RPC `DeletePortofolioBandarmology`:
//! token-scan `portofolio_bandarmology` → hapus partition bila emiten
//! **tidak** ada di `portofolio`.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{Local, NaiveDate};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::DeserializeRow;

use crate::bandarmology_worker::{portofolio_bandarmology_source, BandarmologyDay};

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;
const PAGE_SIZE: i32 = 100;

#[derive(Debug, DeserializeRow)]
struct CurrentMonthWeeksRow {
    broker_summary_current_w1: Option<BandarmologyDay>,
    broker_summary_current_w2: Option<BandarmologyDay>,
    broker_summary_current_w3: Option<BandarmologyDay>,
    broker_summary_current_w4: Option<BandarmologyDay>,
}

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

fn pick_week_day(
    week: u8,
    w1: Option<BandarmologyDay>,
    w2: Option<BandarmologyDay>,
    w3: Option<BandarmologyDay>,
    w4: Option<BandarmologyDay>,
) -> Option<BandarmologyDay> {
    match week {
        1 => w1,
        2 => w2,
        3 => w3,
        _ => w4,
    }
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

async fn load_current_month_weeks(
    session: &Session,
    keyspace: &str,
    agg: &str,
) -> Result<
    (
        Option<BandarmologyDay>,
        Option<BandarmologyDay>,
        Option<BandarmologyDay>,
        Option<BandarmologyDay>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let stmt = session
        .prepare(format!(
            "SELECT broker_summary_current_w1, broker_summary_current_w2, \
             broker_summary_current_w3, broker_summary_current_w4 \
             FROM {keyspace}.bandarmology \
             WHERE agg_tahun_bulan_emiten_name = ? LIMIT 1"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (agg,))
        .await?
        .into_rows_result()?;
    let mut rows = result.rows::<CurrentMonthWeeksRow>()?;
    Ok(if let Some(row) = rows.next().transpose()? {
        (
            row.broker_summary_current_w1,
            row.broker_summary_current_w2,
            row.broker_summary_current_w3,
            row.broker_summary_current_w4,
        )
    } else {
        (None, None, None, None)
    })
}

/// Upsert satu emiten ke `portofolio_bandarmology` dari minggu berjalan di `bandarmology`.
/// Returns `Ok(true)` bila di-write; `Ok(false)` bila sumber minggu kosong / baris bandarmology tidak ada.
pub async fn insert_portofolio_bandarmology_for_emiten(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let emiten = emiten_name.trim().to_ascii_uppercase();
    if emiten.is_empty() {
        return Ok(false);
    }

    let today: NaiveDate = Local::now().date_naive();
    let Some((week, agg)) = portofolio_bandarmology_source(today, &emiten) else {
        return Ok(false);
    };
    let (w1, w2, w3, w4) = load_current_month_weeks(session, keyspace, &agg).await?;

    let Some(day) = pick_week_day(week, w1, w2, w3, w4) else {
        println!(
            "portofolio_bandarmology [{emiten}]: skip — bandarmology `{agg}` / w{week} kosong"
        );
        return Ok(false);
    };

    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio_bandarmology (\
                emiten_name, tahun_bulan_tanggal, bandarmology\
            ) VALUES (?, ?, ?)"
        ))
        .await?;

    session
        .execute_unpaged(&insert, (emiten.as_str(), today, &day))
        .await?;

    println!(
        "portofolio_bandarmology [{emiten}]: upsert {today} dari `{agg}` w{week}"
    );
    Ok(true)
}

/// Upsert banyak emiten; error per-emiten di-log, tidak menghentikan batch.
/// Returns jumlah yang berhasil di-write.
pub async fn insert_portofolio_bandarmology_for_emitens(
    session: &Session,
    keyspace: &str,
    emitens: &[String],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut ok = 0usize;
    for raw in emitens {
        match insert_portofolio_bandarmology_for_emiten(session, keyspace, raw).await {
            Ok(true) => ok += 1,
            Ok(false) => {}
            Err(e) => {
                eprintln!(
                    "portofolio_bandarmology [{}]: gagal: {e}",
                    raw.trim().to_ascii_uppercase()
                );
            }
        }
    }
    Ok(ok)
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
