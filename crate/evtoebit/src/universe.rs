use std::collections::BTreeSet;

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;

const LIST_UNIVERSE: &str = "SELECT code, sector FROM invezgood.stock_list";

const FINANCIAL_KEYWORDS: &[&str] = &[
    "bank", "asuransi", "insurance", "pembiayaan", "financ", "sekuritas", "keuangan",
];

#[derive(Debug, Clone, DeserializeRow)]
struct UniverseDbRow {
    code: String,
    #[scylla(default_when_null)]
    sector: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UniverseRow {
    pub kode: String,
    pub sektor: String,
}

fn is_financial_sector(sektor: &str) -> bool {
    let lower = sektor.to_ascii_lowercase();
    FINANCIAL_KEYWORDS
        .iter()
        .any(|kw| lower.contains(kw))
}

fn normalize_sector(sector: Option<String>) -> Option<String> {
    let sektor = sector?.trim().to_string();
    if sektor.is_empty() {
        None
    } else {
        Some(sektor)
    }
}

/// Daftar sektor unik non-keuangan dari kolom `sector` invezgood.stock_list.
pub async fn load_sectors(session: &Session) -> Result<Vec<String>, String> {
    let rows = session
        .query_iter(LIST_UNIVERSE, &[])
        .await
        .map_err(|e| format!("load_sectors invezgood.stock_list: {e}"))?
        .rows_stream::<UniverseDbRow>()
        .map_err(|e| format!("load_sectors stream: {e}"))?;

    let mut sectors = BTreeSet::new();
    let mut stream = rows;
    while let Some(row) = stream
        .try_next()
        .await
        .map_err(|e| format!("load_sectors row: {e}"))?
    {
        let Some(sektor) = normalize_sector(row.sector) else {
            continue;
        };
        if is_financial_sector(&sektor) {
            continue;
        }
        sectors.insert(sektor);
    }
    Ok(sectors.into_iter().collect())
}

/// Emiten BEI dengan sektor valid dari invezgood.stock_list (non-keuangan, bukan warrant).
pub async fn load_universe(session: &Session) -> Result<Vec<UniverseRow>, String> {
    let allowed_sectors: BTreeSet<String> = load_sectors(session).await?.into_iter().collect();

    let rows = session
        .query_iter(LIST_UNIVERSE, &[])
        .await
        .map_err(|e| format!("load_universe invezgood.stock_list: {e}"))?
        .rows_stream::<UniverseDbRow>()
        .map_err(|e| format!("load_universe stream: {e}"))?;

    let mut out = Vec::new();
    let mut stream = rows;
    while let Some(row) = stream
        .try_next()
        .await
        .map_err(|e| format!("load_universe row: {e}"))?
    {
        let kode = row.code.trim().to_ascii_uppercase();
        if kode.is_empty() || kode.contains('-') {
            continue;
        }
        let Some(sektor) = normalize_sector(row.sector) else {
            continue;
        };
        if is_financial_sector(&sektor) || !allowed_sectors.contains(&sektor) {
            continue;
        }
        out.push(UniverseRow { kode, sektor });
    }
    Ok(out)
}
