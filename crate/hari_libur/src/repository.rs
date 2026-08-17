use chrono::NaiveDate;
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{HariLiburRow, KEYSPACE, TABLE, VIEW_BY_TAHUN};

const FIND_BY_TAHUN: &str = "SELECT date, tahun, name, is_civic, is_religious, is_cuti_bersama, \
    updated_at FROM invezgood.hari_libur_by_tahun WHERE tahun = ?";

const UPSERT: &str = "INSERT INTO invezgood.hari_libur \
    (date, tahun, name, is_civic, is_religious, is_cuti_bersama, updated_at) \
    VALUES (?, ?, ?, ?, ?, ?, ?)";

const UPDATE: &str = "UPDATE invezgood.hari_libur SET tahun = ?, name = ?, is_civic = ?, \
    is_religious = ?, is_cuti_bersama = ?, updated_at = ? WHERE date = ?";

const EXISTS: &str = "SELECT date FROM invezgood.hari_libur WHERE date = ?";

const DELETE: &str = "DELETE FROM invezgood.hari_libur WHERE date = ?";

/// Semua libur satu tahun dari MV (sudah urut `date` ASC sesuai clustering order).
pub async fn find_by_tahun(session: &Session, tahun: &str) -> Result<Vec<HariLiburRow>, String> {
    let rows = session
        .query_iter(FIND_BY_TAHUN, (tahun,))
        .await
        .map_err(|e| format!("find_by_tahun {KEYSPACE}.{VIEW_BY_TAHUN} tahun={tahun}: {e}"))?
        .rows_stream::<HariLiburRow>()
        .map_err(|e| format!("find_by_tahun stream {KEYSPACE}.{VIEW_BY_TAHUN}: {e}"))?;

    rows.try_collect()
        .await
        .map_err(|e| format!("find_by_tahun rows {KEYSPACE}.{VIEW_BY_TAHUN} tahun={tahun}: {e}"))
}

/// Upsert satu tanggal libur (PK `date`, jadi tanggal sama akan ditimpa).
pub async fn upsert(session: &Session, row: &HariLiburRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                row.date,
                row.tahun.as_ref(),
                row.name.as_ref(),
                row.is_civic,
                row.is_religious,
                row.is_cuti_bersama,
                row.updated_at,
            ),
        )
        .await
        .map_err(|e| format!("upsert {KEYSPACE}.{TABLE} date={}: {e}", row.date))?;
    Ok(())
}

/// Update kolom non-PK. `Ok(false)` bila tanggal belum ada (UPDATE Scylla = upsert,
/// jadi keberadaan baris harus dicek lebih dulu).
pub async fn update(session: &Session, row: &HariLiburRow) -> Result<bool, String> {
    if !date_exists(session, row.date).await? {
        return Ok(false);
    }

    session
        .query_unpaged(
            UPDATE,
            (
                row.tahun.as_ref(),
                row.name.as_ref(),
                row.is_civic,
                row.is_religious,
                row.is_cuti_bersama,
                row.updated_at,
                row.date,
            ),
        )
        .await
        .map_err(|e| format!("update {KEYSPACE}.{TABLE} date={}: {e}", row.date))?;
    Ok(true)
}

pub async fn date_exists(session: &Session, date: NaiveDate) -> Result<bool, String> {
    let mut rows = session
        .query_iter(EXISTS, (date,))
        .await
        .map_err(|e| format!("date_exists {KEYSPACE}.{TABLE} date={date}: {e}"))?
        .rows_stream::<(NaiveDate,)>()
        .map_err(|e| format!("date_exists stream {KEYSPACE}.{TABLE}: {e}"))?;

    Ok(rows
        .try_next()
        .await
        .map_err(|e| format!("date_exists row {KEYSPACE}.{TABLE} date={date}: {e}"))?
        .is_some())
}

/// Hapus satu tanggal libur. `Ok(false)` bila tanggal tidak ada di tabel.
pub async fn delete(session: &Session, date: NaiveDate) -> Result<bool, String> {
    if !date_exists(session, date).await? {
        return Ok(false);
    }

    session
        .query_unpaged(DELETE, (date,))
        .await
        .map_err(|e| format!("delete {KEYSPACE}.{TABLE} date={date}: {e}"))?;
    Ok(true)
}
