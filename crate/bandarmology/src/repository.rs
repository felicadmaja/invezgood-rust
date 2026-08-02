use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{
    agg_key, BandarmologyHarianRow, BandarmologyRow, PortofolioBandarmologyRow, KEYSPACE,
    TABLE, TABLE_HARIAN, TABLE_PORTOFOLIO,
};

const FIND_BY_AGG_KEY: &str = "SELECT agg_tahun_bulan_emiten_name, emiten_name, tahun_bulan, \
    broker_summary, broker_summary_current_w1, broker_summary_current_w2, \
    broker_summary_current_w3, broker_summary_current_w4, updated_at \
    FROM stockbit.bandarmology WHERE agg_tahun_bulan_emiten_name = ?";

const FIND_HARIAN_BY_CODE_DATE: &str = "SELECT emiten_name, tahun_bulan_tanggal, \
    broker_summary_harian, updated_at \
    FROM stockbit.bandarmology_harian WHERE emiten_name = ? AND tahun_bulan_tanggal = ?";

const FIND_HARIAN_LATEST: &str = "SELECT emiten_name, tahun_bulan_tanggal, \
    broker_summary_harian, updated_at \
    FROM stockbit.bandarmology_harian WHERE emiten_name = ? LIMIT 1";

const FIND_PORTOFOLIO_BY_CODE_DATE: &str = "SELECT emiten_name, tahun_bulan_tanggal, bandarmology \
    FROM stockbit.portofolio_bandarmology WHERE emiten_name = ? AND tahun_bulan_tanggal = ?";

const FIND_PORTOFOLIO_LATEST: &str = "SELECT emiten_name, tahun_bulan_tanggal, bandarmology \
    FROM stockbit.portofolio_bandarmology WHERE emiten_name = ? LIMIT 1";

pub async fn find_by_code_and_month(
    session: &Session,
    code: &str,
    tahun_bulan: &str,
) -> Result<Option<BandarmologyRow>, String> {
    let key = agg_key(tahun_bulan, code);
    let mut rows = session
        .query_iter(FIND_BY_AGG_KEY, (&key,))
        .await
        .map_err(|e| format!("find_by_code_and_month {KEYSPACE}.{TABLE} key={key}: {e}"))?
        .rows_stream::<BandarmologyRow>()
        .map_err(|e| format!("find_by_code_and_month stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_code_and_month row {KEYSPACE}.{TABLE}: {e}"))
}

pub async fn find_harian_by_code(
    session: &Session,
    code: &str,
    trade_date: Option<chrono::NaiveDate>,
) -> Result<Option<BandarmologyHarianRow>, String> {
    let mut rows = if let Some(date) = trade_date {
        session
            .query_iter(FIND_HARIAN_BY_CODE_DATE, (code, date))
            .await
            .map_err(|e| {
                format!(
                    "find_harian_by_code {KEYSPACE}.{TABLE_HARIAN} code={code} date={date}: {e}"
                )
            })?
    } else {
        session
            .query_iter(FIND_HARIAN_LATEST, (code,))
            .await
            .map_err(|e| {
                format!("find_harian_latest {KEYSPACE}.{TABLE_HARIAN} code={code}: {e}")
            })?
    }
    .rows_stream::<BandarmologyHarianRow>()
    .map_err(|e| format!("find_harian_by_code stream {KEYSPACE}.{TABLE_HARIAN}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_harian_by_code row {KEYSPACE}.{TABLE_HARIAN}: {e}"))
}

pub async fn find_portofolio_by_code(
    session: &Session,
    code: &str,
    trade_date: Option<chrono::NaiveDate>,
) -> Result<Option<PortofolioBandarmologyRow>, String> {
    let mut rows = if let Some(date) = trade_date {
        session
            .query_iter(FIND_PORTOFOLIO_BY_CODE_DATE, (code, date))
            .await
            .map_err(|e| {
                format!(
                    "find_portofolio_by_code {KEYSPACE}.{TABLE_PORTOFOLIO} code={code} date={date}: {e}"
                )
            })?
    } else {
        session
            .query_iter(FIND_PORTOFOLIO_LATEST, (code,))
            .await
            .map_err(|e| {
                format!(
                    "find_portofolio_latest {KEYSPACE}.{TABLE_PORTOFOLIO} code={code}: {e}"
                )
            })?
    }
    .rows_stream::<PortofolioBandarmologyRow>()
    .map_err(|e| format!("find_portofolio_by_code stream {KEYSPACE}.{TABLE_PORTOFOLIO}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_portofolio_by_code row {KEYSPACE}.{TABLE_PORTOFOLIO}: {e}"))
}
