use scylla::client::session::Session;

use crate::model::{ChartRow, KEYSPACE, TABLE};

const UPSERT: &str = "INSERT INTO invezgood.chart \
    (code, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)";

pub async fn upsert(session: &Session, row: &ChartRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                row.code.as_str(),
                row.date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} code={} date={}: {e}",
                row.code, row.date
            )
        })?;
    Ok(())
}
