//! ```bash
//! cargo run -p portofolio
//! ```
//! Jalankan gRPC server portofolio (default port 50054).
//! Semua RPC wajib JWT (`Authorization: Bearer <token>` dari `user.Login`).

use portofolio::portofolio_server::PortofolioServer;
use portofolio::PortofolioService;
use tonic_reflection::server::Builder as ReflectionBuilder;
use user::AuthInterceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workspace_env = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    if std::env::var("JWT_SECRET").unwrap_or_default().is_empty() {
        return Err("JWT_SECRET wajib diisi di .env (sama dengan crate user)".into());
    }

    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50054".to_string())
        .parse()?;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let session = portofolio::session().await?;
    let svc = PortofolioServer::with_interceptor(
        PortofolioService::new(session),
        AuthInterceptor,
    );

    let reflection_svc = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(portofolio::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| format!("reflection: {e}"))?;

    eprintln!("portofolio gRPC listening on {addr} (JWT required, reflection enabled)");
    tonic::transport::Server::builder()
        .add_service(svc)
        .add_service(reflection_svc)
        .serve(addr)
        .await?;

    Ok(())
}
