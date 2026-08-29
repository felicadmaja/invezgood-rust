use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;

const LIST_UNIVERSE: &str =
    "SELECT code, sector, sub_sector FROM invezgood.stock_list";

const FINANCIAL_KEYWORDS: &[&str] = &[
    "bank", "asuransi", "insurance", "pembiayaan", "financ", "sekuritas",
];

#[derive(Debug, Clone, DeserializeRow)]
struct UniverseDbRow {
    code: String,
    #[scylla(default_when_null)]
    sector: Option<String>,
    #[scylla(default_when_null)]
    sub_sector: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UniverseRow {
    pub kode: String,
    pub sektor: String,
    pub sub_sektor: String,
}

fn is_financial_subsector(sub_sektor: &str) -> bool {
    let lower = sub_sektor.to_ascii_lowercase();
    FINANCIAL_KEYWORDS
        .iter()
        .any(|kw| lower.contains(kw))
}

pub async fn load_universe(session: &Session) -> Result<Vec<UniverseRow>, String> {
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
        let sektor = row.sector.unwrap_or_default().trim().to_string();
        let sub_sektor = row
            .sub_sector
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| sektor.clone());
        if is_financial_subsector(&sub_sektor) {
            continue;
        }
        out.push(UniverseRow {
            kode,
            sektor,
            sub_sektor,
        });
    }
    Ok(out)
}
