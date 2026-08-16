use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{HariLiburRow, KEYSPACE, VIEW_BY_TAHUN};

const FIND_BY_TAHUN: &str = "SELECT date, tahun, name, is_civic, is_religious, is_cuti_bersama, \
    updated_at FROM invezgood.hari_libur_by_tahun WHERE tahun = ?";

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
