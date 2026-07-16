//! ```bash
//! cargo run -p user
//! ```
//! Jalankan gRPC User service (Login) — default port 50055.
//!
//! Env: `JWT_SECRET`, `SCYLLA_*`, opsional `USER_GRPC_PORT` (default 50055).

use user::user_server::UserServer;
use user::UserService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workspace_env = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    if std::env::var("JWT_SECRET").unwrap_or_default().is_empty() {
        return Err("JWT_SECRET wajib diisi di .env".into());
    }

    let port: u16 = std::env::var("USER_GRPC_PORT")
        .unwrap_or_else(|_| "50055".to_string())
        .parse()?;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let session = user::session().await?;
    let svc = UserServer::new(UserService::new(session));

    eprintln!("user gRPC (Login) listening on {addr}");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
