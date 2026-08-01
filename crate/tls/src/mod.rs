//! TLS: server HTTPS support for gRPC.
//!
//! Membaca sertifikat dari `TLS_CERT_DIR` (default: `src/certificate`) untuk menerima koneksi HTTPS.
//! Set `USE_TLS=true` di `.env` untuk mengaktifkan.

use std::path::Path;
use tonic::transport::ServerTlsConfig;

/// Membaca USE_TLS dari env. Default: false (plaintext).
pub fn use_tls_from_env() -> bool {
    std::env::var("USE_TLS")
        .unwrap_or_else(|_| "false".into())
        .parse::<bool>()
        .unwrap_or(false)
}

/// Memuat konfigurasi TLS: sertifikat server + private key.
/// Env: TLS_CERT_DIR, TLS_CERT_FILE, TLS_KEY_FILE.
///
/// Default paths:
/// - TLS_CERT_DIR: src/certificate
/// - TLS_CERT_FILE: _.mariban.com_chain.crt (full chain)
/// - TLS_KEY_FILE: _.mariban.com.key
pub fn load_tls_config() -> Result<ServerTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let cert_dir = std::env::var("TLS_CERT_DIR").unwrap_or_else(|_| "src/certificate".into());
    let cert_file =
        std::env::var("TLS_CERT_FILE").unwrap_or_else(|_| "_.mariban.com_chain.crt".into());
    let key_file = std::env::var("TLS_KEY_FILE").unwrap_or_else(|_| "_.mariban.com.key".into());

    let cert_path = resolve_path(&cert_dir, &cert_file);
    let key_path = resolve_path(&cert_dir, &key_file);

    let cert_pem = std::fs::read(&cert_path)
        .map_err(|e| format!("failed to read TLS cert {}: {}", cert_path, e))?;
    let key_pem = std::fs::read(&key_path)
        .map_err(|e| format!("failed to read TLS key {}: {}", key_path, e))?;

    let identity = tonic::transport::Identity::from_pem(&cert_pem, &key_pem);
    let tls_config = ServerTlsConfig::new().identity(identity);
    Ok(tls_config)
}

fn resolve_path(cert_dir: &str, file: &str) -> String {
    if Path::new(file).is_absolute() {
        file.to_string()
    } else {
        format!(
            "{}/{}",
            cert_dir.trim_end_matches('/'),
            file.trim_start_matches('/')
        )
    }
}
