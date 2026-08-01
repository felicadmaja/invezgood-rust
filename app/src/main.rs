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
//! Response/request gRPC gzip: aktif default (client perlu `grpc-accept-encoding: gzip`).

mod ready_auto_scrape;

use bandarmology::bandarmology_server::BandarmologyServer;
use bandarmology::BandarmologyService;
use broker::broker_server::BrokerServer;
use broker::BrokerService;
use emiten_list::emiten_list_server::EmitenListServer;
use emiten_list::EmitenListService;
use emiten_trending::emiten_trending_server::EmitenTrendingServer;
use emiten_trending::EmitenTrendingService;
use emiten_trending_count::emiten_trending_count_server::EmitenTrendingCountServer;
use emiten_trending_count::EmitenTrendingCountService;
use gcs::gcs_server::GcsServer;
use gcs::GcsGrpcService;
use pending_order::pending_order_server::PendingOrderServer;
use pending_order::PendingOrderService;
use portofolio::portofolio_server::PortofolioServer;
use portofolio::PortofolioService;
use portofolio_bandarmology::portofolio_bandarmology_server::PortofolioBandarmologyServer;
use portofolio_bandarmology::PortofolioBandarmologyService;
use portofolio_catatan::portofolio_catatan_server::PortofolioCatatanServer;
use portofolio_catatan::PortofolioCatatanService;
use portofolio_equity::portofolio_equity_server::PortofolioEquityServer;
use portofolio_equity::PortofolioEquityService;
use portofolio_history::portofolio_history_server::PortofolioHistoryServer;
use portofolio_history::PortofolioHistoryService;
use realtime_price::realtime_price_server::RealtimePriceServer;
use realtime_price::RealtimePriceService;
use tonic_reflection::server::Builder as ReflectionBuilder;
use user::user_server::UserServer;
use user::{AuthInterceptor, UserService};
use wyckoff_glossary::wyckoff_glossary_server::WyckoffGlossaryServer;
use wyckoff_glossary::WyckoffGlossaryService;
use tonic::codec::CompressionEncoding;
use tonic::codegen::InterceptedService;

/// Aktifkan gzip request/response pada server tonic generated.
macro_rules! gzip_svc {
    ($svc:expr) => {{
        $svc.accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    }};
}

/// Server + gzip, lalu AuthInterceptor (gzip harus sebelum interceptor).
macro_rules! auth_gzip_svc {
    ($server:ident, $svc:expr) => {{
        InterceptedService::new(gzip_svc!($server::new($svc)), AuthInterceptor)
    }};
}

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
    let portofolio_bandarmology_svc = PortofolioBandarmologyService::new(session.clone());
    let portofolio_catatan_svc = PortofolioCatatanService::new(session.clone());
    let portofolio_equity_svc = PortofolioEquityService::new(session.clone());
    let portofolio_history_svc = PortofolioHistoryService::new(session.clone());
    let emiten_trending_svc = EmitenTrendingService::new(session.clone());
    let emiten_trending_count_svc = EmitenTrendingCountService::new(session.clone());
    let bandarmology_svc = BandarmologyService::new(session.clone());
    let emiten_list_svc = EmitenListService::new(session.clone());
    let broker_svc = BrokerService::new(session.clone());
    let pending_order_svc = PendingOrderService::new(session.clone());
    let gcs_svc = GcsGrpcService::from_env()
        .map_err(|e| format!("GCS env: {e}"))?;
    let wyckoff_glossary_svc = WyckoffGlossaryService::new(session.clone());
    let realtime_price_svc = RealtimePriceService::new();

    ready_auto_scrape::spawn_on_stockbit_ready(
        user_svc.readiness_poller(),
        PortofolioService::new(session.clone()),
        PendingOrderService::new(session.clone()),
        EmitenTrendingService::new(session.clone()),
    );

    // Semua warm_prepared harus selesai di sini — sebelum router.serve.
    // Binary utama wajib gagal startup bila warm/prepare gagal.
    tokio::try_join!(
        user_svc.warm_prepared(),
        portofolio_svc.warm_prepared(),
        portofolio_bandarmology_svc.warm_prepared(),
        portofolio_catatan_svc.warm_prepared(),
        portofolio_equity_svc.warm_prepared(),
        portofolio_history_svc.warm_prepared(),
        emiten_trending_svc.warm_prepared(),
        emiten_trending_count_svc.warm_prepared(),
        bandarmology_svc.warm_prepared(),
        emiten_list_svc.warm_prepared(),
        broker_svc.warm_prepared(),
        pending_order_svc.warm_prepared(),
        wyckoff_glossary_svc.warm_prepared(),
    )
    .map_err(|e| format!("Gagal memanaskan statement database: {e}"))?;
    println!(
        "OK: prepared statements siap (user, portofolio, portofolio_bandarmology, portofolio_catatan, portofolio_equity, portofolio_history, emiten_trending, emiten_trending_count, bandarmology, emiten_list, broker, pending_order, wyckoff_glossary)"
    );

    let user_svc = gzip_svc!(UserServer::new(user_svc));
    let portofolio_svc = auth_gzip_svc!(PortofolioServer, portofolio_svc);
    let portofolio_bandarmology_svc =
        auth_gzip_svc!(PortofolioBandarmologyServer, portofolio_bandarmology_svc);
    let portofolio_catatan_svc = auth_gzip_svc!(PortofolioCatatanServer, portofolio_catatan_svc);
    let portofolio_equity_svc = auth_gzip_svc!(PortofolioEquityServer, portofolio_equity_svc);
    let portofolio_history_svc = auth_gzip_svc!(PortofolioHistoryServer, portofolio_history_svc);
    let emiten_trending_svc = auth_gzip_svc!(EmitenTrendingServer, emiten_trending_svc);
    let emiten_trending_count_svc =
        auth_gzip_svc!(EmitenTrendingCountServer, emiten_trending_count_svc);
    let bandarmology_svc = auth_gzip_svc!(BandarmologyServer, bandarmology_svc);
    let emiten_list_svc = auth_gzip_svc!(EmitenListServer, emiten_list_svc);
    let broker_svc = auth_gzip_svc!(BrokerServer, broker_svc);
    let pending_order_svc = auth_gzip_svc!(PendingOrderServer, pending_order_svc);
    let gcs_svc = auth_gzip_svc!(GcsServer, gcs_svc);
    let wyckoff_glossary_svc = auth_gzip_svc!(WyckoffGlossaryServer, wyckoff_glossary_svc);
    let realtime_price_svc = auth_gzip_svc!(RealtimePriceServer, realtime_price_svc);

    let reflection_svc = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(user::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio_bandarmology::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio_catatan::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio_equity::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(portofolio_history::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(emiten_trending::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(emiten_trending_count::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(bandarmology::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(emiten_list::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(broker::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(pending_order::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(gcs::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(wyckoff_glossary::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(realtime_price::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| format!("reflection: {e}"))?;
    let reflection_svc = gzip_svc!(reflection_svc);

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

    println!("stockbit_ws gRPC listening on {addr} (reflection enabled, gzip default)");
    builder
        .add_service(user_svc)
        .add_service(portofolio_svc)
        .add_service(portofolio_bandarmology_svc)
        .add_service(portofolio_catatan_svc)
        .add_service(portofolio_equity_svc)
        .add_service(portofolio_history_svc)
        .add_service(emiten_trending_svc)
        .add_service(emiten_trending_count_svc)
        .add_service(bandarmology_svc)
        .add_service(emiten_list_svc)
        .add_service(broker_svc)
        .add_service(pending_order_svc)
        .add_service(gcs_svc)
        .add_service(wyckoff_glossary_svc)
        .add_service(realtime_price_svc)
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
