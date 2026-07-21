//! Salin snapshot bandarmology minggu berjalan → `portofolio_bandarmology`.
//!
//! Alur sama RPC `InsertPortofolioBandarmology`:
//! - Baca `bandarmology` PK `YYYY-MM_EMITEN` (bulan hari ini)
//! - Pilih `broker_summary_current_w1`…`w4` menurut tanggal hari ini:
//!   1–8 → w1, 9–15 → w2, 16–22 → w3, 23–31 → w4
//! - Upsert `portofolio_bandarmology` (emiten_name, today, day)

use chrono::{Datelike, Local, NaiveDate};
use scylla::client::session::Session;
use scylla::DeserializeRow;

use crate::bandarmology_worker::{
    bandarmology_agg_key, BandarmologyDay,
};

#[derive(Debug, DeserializeRow)]
struct CurrentMonthWeeksRow {
    broker_summary_current_w1: Option<BandarmologyDay>,
    broker_summary_current_w2: Option<BandarmologyDay>,
    broker_summary_current_w3: Option<BandarmologyDay>,
    broker_summary_current_w4: Option<BandarmologyDay>,
}

/// Minggu aktif menurut tanggal kalender (aturan `InsertPortofolioBandarmology`).
pub fn current_week_slot(day_of_month: u32) -> u8 {
    match day_of_month {
        1..=8 => 1,
        9..=15 => 2,
        16..=22 => 3,
        _ => 4, // 23–31
    }
}

fn pick_week_day(
    day_of_month: u32,
    w1: Option<BandarmologyDay>,
    w2: Option<BandarmologyDay>,
    w3: Option<BandarmologyDay>,
    w4: Option<BandarmologyDay>,
) -> Option<BandarmologyDay> {
    match current_week_slot(day_of_month) {
        1 => w1,
        2 => w2,
        3 => w3,
        _ => w4,
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
    let agg = bandarmology_agg_key(today, &emiten);
    let (w1, w2, w3, w4) = load_current_month_weeks(session, keyspace, &agg).await?;

    let Some(day) = pick_week_day(today.day(), w1, w2, w3, w4) else {
        println!(
            "portofolio_bandarmology [{emiten}]: skip — bandarmology `{agg}` / w{} kosong",
            current_week_slot(today.day())
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
        "portofolio_bandarmology [{emiten}]: upsert {today} dari w{}",
        current_week_slot(today.day())
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
