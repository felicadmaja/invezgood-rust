//! Client stream upload inlineXBRL.zip ke UploadZip RPC.
//!
//! Env: GRPC_EMAIL, GRPC_PASSWORD, GRPC_ADDR (default https://127.0.0.1:50054)
//!
//! Usage:
//!   cargo run -p xlbr_laporan_keuangan --example grpc_upload_zip -- path/to/inlineXBRL.zip

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use user::pb::user_client::UserClient;
use user::pb::LoginRequest;
use xlbr_laporan_keuangan::pb::xlbr_laporan_keuangan_client::XlbrLaporanKeuanganClient;
use xlbr_laporan_keuangan::pb::UploadZipChunk;

const CHUNK_SIZE: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();

    let path = std::env::args()
        .nth(1)
        .ok_or("usage: grpc_upload_zip <path-to.zip>")?;
    let bytes = tokio::fs::read(&path).await?;

    let addr = std::env::var("GRPC_ADDR").unwrap_or_else(|_| "https://127.0.0.1:50054".into());
    let tls_domain = std::env::var("GRPC_TLS_DOMAIN").unwrap_or_else(|_| "mariban.com".into());
    let email = std::env::var("GRPC_EMAIL")
        .or_else(|_| std::env::var("STOCKBIT_EMAIL"))
        .map_err(|_| "set GRPC_EMAIL or STOCKBIT_EMAIL")?;
    let password = std::env::var("GRPC_PASSWORD")
        .or_else(|_| std::env::var("STOCKBIT_PASSWORD"))
        .map_err(|_| "set GRPC_PASSWORD or STOCKBIT_PASSWORD")?;

    let channel = Channel::from_shared(addr)?
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .with_native_roots()
                .domain_name(tls_domain),
        )?
        .connect()
        .await?;

    let mut user_client = UserClient::new(channel.clone());
    let login = user_client
        .login(LoginRequest { email, password })
        .await?
        .into_inner();

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    for chunk in bytes.chunks(CHUNK_SIZE) {
        tx.send(UploadZipChunk {
            data: chunk.to_vec(),
        })
        .await
        .ok();
    }
    drop(tx);

    let mut meta = tonic::metadata::MetadataMap::new();
    meta.insert(
        "authorization",
        format!("Bearer {}", login.token)
            .parse()
            .expect("authorization header"),
    );

    let mut xlbr_client = XlbrLaporanKeuanganClient::new(channel);
    let mut req = tonic::Request::new(ReceiverStream::new(rx));
    *req.metadata_mut() = meta;

    let resp = xlbr_client.upload_zip(req).await?.into_inner();
    println!(
        "success={} {} {} {} {}",
        resp.success, resp.code, resp.fiscal_year, resp.quarter, resp.message
    );
    Ok(())
}
