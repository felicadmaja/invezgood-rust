//! Binary utama stockbit_ws — satu proses gRPC untuk semua crate layanan.
//!
//! ```bash
//! cargo run -p stockbit_ws
//! # atau
//! cargo build --release && ./target/release/stockbit_ws
//! ```
//!
//! Env: `JWT_SECRET`, `SCYLLA_*`, opsional `HOST` / `GRPC_PORT` (default `0.0.0.0:50054`).
//! TLS (opsional): `USE_TLS=true`, `TLS_CERT_DIR` (default folder certificate mrgs),
//! `TLS_CERT_FILE`, `TLS_KEY_FILE`.

use emiten_trending::emiten_trending_server::EmitenTrendingServer;
use emiten_trending::EmitenTrendingService;
use portofolio::portofolio_server::PortofolioServer;
use portofolio::PortofolioService;
use tonic_reflection::server::Builder as ReflectionBuilder;
use user::user_server::UserServer;
use user::{AuthInterceptor, UserService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    load_dotenv();

    if std::env::var("JWT_SECRET").unwrap_or_default().is_empty() {
        return Err("JWT_SECRET wajib diisi di .env".into());
    }

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50054".into())
        .parse()
        .map_err(|_| "GRPC_PORT harus berupa angka")?;
    let addr: std::net::SocketAddr = format!("{host}:{port}").parse()?;

    let session = user::session().await?;

    let user_svc = UserService::new(session.clone());
    let portofolio_svc = PortofolioService::new(session.clone());
    let emiten_trending_svc = EmitenTrendingService::new(session);

    // Semua warm_prepared harus selesai di sini — sebelum router.serve.
    // Binary utama wajib gagal startup bila warm/prepare gagal.
    tokio::try_join!(
        user_svc.warm_prepared(),
        portofolio_svc.warm_prepared(),
        emiten_trending_svc.warm_prepared(),
    )
    .map_err(|e| format!("Gagal memanaskan statement database: {e}"))?;
    println!("OK: prepared statements siap (user, portofolio, emiten_trending)");

    let user_svc = UserServer::new(user_svc);
    let portofolio_svc =
        PortofolioServer::with_interceptor(portofolio_svc, AuthInterceptor);
    let emiten_trending_svc =
        EmitenTrendingServer::with_interceptor(emiten_trending_svc, AuthInterceptor);

    let reflection_svc = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(user::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(emiten_trending::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| format!("reflection: {e}"))?;

    let mut builder = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)));

    if tls::use_tls_from_env() {
        let tls_config = tls::load_tls_config()?;
        println!("TLS enabled: gRPC server accepting HTTPS connections");
        builder = builder.tls_config(tls_config)?;
    } else {
        println!("TLS disabled (USE_TLS=false): plaintext gRPC");
    }

    println!("stockbit_ws gRPC listening on {addr} (reflection enabled)");
    builder
        .add_service(user_svc)
        .add_service(portofolio_svc)
        .add_service(emiten_trending_svc)
        .add_service(reflection_svc)
        .serve_with_shutdown(addr, grpc_shutdown_signal())
        .await?;

    Ok(())
}

fn load_dotenv() {
    let workspace_env = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
        return;
    }
    dotenvy::dotenv().ok();
}

async fn grpc_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            () = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }

    println!("shutdown signal diterima, menghentikan gRPC server...");
}
