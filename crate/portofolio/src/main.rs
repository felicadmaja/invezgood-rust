//! ```bash
//! cargo run -p portofolio
//! ```
//! Jalankan gRPC server portofolio (default port 50054).

use portofolio::portofolio_server::PortofolioServer;
use portofolio::PortofolioService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workspace_env = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50054".to_string())
        .parse()?;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let session = portofolio::session().await?;
    let svc = PortofolioServer::new(PortofolioService::new(session));

    eprintln!("portofolio gRPC listening on {addr}");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
