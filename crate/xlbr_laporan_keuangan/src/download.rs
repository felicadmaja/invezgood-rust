//! Unduh arsip inlineXBRL tersimpan di `src/downloaded_xbrl/{CODE}/`.

use std::io::Write;
use std::path::PathBuf;

use zip::write::SimpleFileOptions;
use zip::ZipWriter;

pub const STREAM_CHUNK_BYTES: usize = 64 * 1024;

pub fn emiten_download_dir(code: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/downloaded_xbrl")
        .join(code.trim().to_ascii_uppercase())
}

pub async fn list_emiten_zip_paths(code: &str) -> Result<Vec<PathBuf>, String> {
    let code = code.trim().to_ascii_uppercase();
    if code.is_empty() {
        return Err("code wajib diisi".into());
    }

    let dir = emiten_download_dir(&code);
    let prefix = format!("inlineXBRL-{code}-");
    let mut paths = Vec::new();

    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .map_err(|e| format!("folder {} tidak ada: {e}", dir.display()))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("baca folder {}: {e}", dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(".zip") {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}

/// Parse `2026-Q1` → `(2026, "Q1")`.
pub fn parse_tahun_quarter(raw: &str) -> Result<(i32, String), String> {
    let s = raw.trim().to_ascii_uppercase();
    let (year_str, quarter) = s.split_once('-').ok_or_else(|| {
        format!("tahun_quarter invalid: {raw} (contoh 2026-Q1)")
    })?;
    let year: i32 = year_str
        .parse()
        .map_err(|_| format!("tahun invalid dalam tahun_quarter: {raw}"))?;
    if !matches!(quarter, "Q1" | "Q2" | "Q3" | "Q4") {
        return Err(format!("quarter invalid dalam tahun_quarter: {raw}"));
    }
    Ok((year, quarter.to_string()))
}

pub fn emiten_zip_path(code: &str, fiscal_year: i32, quarter: &str) -> PathBuf {
    let code = code.trim().to_ascii_uppercase();
    emiten_download_dir(&code).join(format!("inlineXBRL-{code}-{fiscal_year}-{quarter}.zip"))
}

pub async fn read_emiten_zip(code: &str, tahun_quarter: &str) -> Result<(Vec<u8>, String), String> {
    let code = code.trim().to_ascii_uppercase();
    let (year, quarter) = parse_tahun_quarter(tahun_quarter)?;
    let path = emiten_zip_path(&code, year, &quarter);
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| format!("baca {}: {e}", path.display()))?;
    let message = format!("{code} {year}-{quarter} {} byte", bytes.len());
    Ok((bytes, message))
}

pub async fn resolve_emiten_download(
    code: &str,
    tahun_quarters: &[String],
) -> Result<(Vec<u8>, String), String> {
    let selected: Vec<String> = tahun_quarters
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if selected.is_empty() {
        return bundle_emiten_zips(code).await;
    }
    if selected.len() == 1 {
        return read_emiten_zip(code, &selected[0]).await;
    }

    let mut paths = Vec::with_capacity(selected.len());
    for tq in &selected {
        let (year, quarter) = parse_tahun_quarter(tq)?;
        paths.push(emiten_zip_path(code, year, &quarter));
    }
    bundle_zip_paths(code, &paths).await
}

async fn bundle_zip_paths(code: &str, paths: &[PathBuf]) -> Result<(Vec<u8>, String), String> {
    let code = code.trim().to_ascii_uppercase();
    if paths.is_empty() {
        return Err(format!("tidak ada inlineXBRL zip untuk {code}"));
    }

    let mut cursor = std::io::Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut cursor);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        for path in paths {
            let name = path
                .file_name()
                .ok_or_else(|| format!("nama file invalid: {}", path.display()))?
                .to_string_lossy()
                .to_string();
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|e| format!("baca {}: {e}", path.display()))?;
            writer
                .start_file(&name, options)
                .map_err(|e| format!("zip start {name}: {e}"))?;
            writer
                .write_all(&bytes)
                .map_err(|e| format!("zip write {name}: {e}"))?;
        }
        writer
            .finish()
            .map_err(|e| format!("zip finish: {e}"))?;
    }

    let bundled = cursor.into_inner();
    let message = format!(
        "{code}: {} file inlineXBRL → bundle {} byte",
        paths.len(),
        bundled.len()
    );
    Ok((bundled, message))
}

pub async fn bundle_emiten_zips(code: &str) -> Result<(Vec<u8>, String), String> {
    let code = code.trim().to_ascii_uppercase();
    let paths = list_emiten_zip_paths(&code).await?;
    if paths.is_empty() {
        return Err(format!("tidak ada inlineXBRL zip untuk {code}"));
    }
    bundle_zip_paths(&code, &paths).await
}
