//! ```bash
//! cargo run -p user
//! ```
//! Jalankan gRPC User service (Login) saja — debug. Produksi pakai `stockbit_ws` (satu port).
//!
//! Env: `JWT_SECRET`, `SCYLLA_*`, opsional `GRPC_PORT` (default 50054).

use tonic_reflection::server::Builder as ReflectionBuilder;
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

    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50054".to_string())
        .parse()?;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let session = user::session().await?;
    let svc = UserServer::new(UserService::new(session));

    let reflection_svc = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(user::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| format!("reflection: {e}"))?;

    eprintln!("user gRPC (Login) listening on {addr} (reflection enabled)");
    tonic::transport::Server::builder()
        .add_service(svc)
        .add_service(reflection_svc)
        .serve(addr)
        .await?;

    Ok(())
}
