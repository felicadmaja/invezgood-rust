//! Login via user.User/Login (email/password dari env) lalu panggil UploadFromUrl.
//!
//! Env: GRPC_EMAIL, GRPC_PASSWORD (atau STOCKBIT_EMAIL / STOCKBIT_PASSWORD), GRPC_ADDR (default localhost:50054)
//!
//! Usage:
//!   cargo run -p xlbr_laporan_keuangan --example grpc_upload_from_url -- <url>

use tonic::transport::Channel;
use user::pb::user_client::UserClient;
use user::pb::LoginRequest;
use xlbr_laporan_keuangan::pb::xlbr_laporan_keuangan_client::XlbrLaporanKeuanganClient;
use xlbr_laporan_keuangan::pb::UploadXlbrFromUrlRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();

    let url = std::env::args().nth(1).ok_or("usage: grpc_upload_from_url <url>")?;
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

    let token = login.token;
    let mut meta = tonic::metadata::MetadataMap::new();
    meta.insert(
        "authorization",
        format!("Bearer {token}").parse().expect("authorization header"),
    );

    let mut xlbr_client = XlbrLaporanKeuanganClient::new(channel);
    let mut req = tonic::Request::new(UploadXlbrFromUrlRequest { url });
    *req.metadata_mut() = meta;

    let resp = xlbr_client.upload_from_url(req).await?.into_inner();
    println!(
        "success={} {} {} {} {}",
        resp.success, resp.code, resp.fiscal_year, resp.quarter, resp.message
    );
    Ok(())
}
