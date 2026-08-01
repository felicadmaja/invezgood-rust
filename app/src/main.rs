//! Entry point — daftarkan semua layanan gRPC dari crate modul di sini.

use stock_list::{connect, StockListServer, StockListService};
use top_gainer_loser::{TopGainerLoserServer, TopGainerLoserService};
use user::{UserServer, UserService};
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;

pub mod pb {
    tonic::include_proto!("invezgood");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("invezgood_descriptor");
}

use pb::invezgood_server::{Invezgood, InvezgoodServer};
use pb::{PingRequest, PingResponse};

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: &str = "50054";

#[derive(Default)]
struct InvezgoodService;

#[tonic::async_trait]
impl Invezgood for InvezgoodService {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        let message = request.into_inner().message;
        Ok(tonic::Response::new(PingResponse {
            message: format!("pong: {message}"),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();

    let host = std::env::var("HOST").unwrap_or_else(|_| DEFAULT_HOST.into());
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| DEFAULT_PORT.into());
    let addr = format!("{host}:{port}").parse()?;

    let session = connect().await?;
    let stock_list = StockListService::new(session.clone())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let top_gainer_loser = TopGainerLoserService::new(session.clone());
    let user = UserService::new(session);

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(stock_list::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(user::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(top_gainer_loser::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let mut builder = Server::builder();

    if tls::use_tls_from_env() {
        let tls_config = tls::load_tls_config()?;
        println!("TLS enabled: gRPC server accepting HTTPS connections");
        builder = builder.tls_config(tls_config)?;
    }

    println!("invezgood gRPC listening on {addr} (reflection enabled)");

    builder
        .add_service(reflection)
        .add_service(InvezgoodServer::new(InvezgoodService))
        .add_service(StockListServer::new(stock_list))
        .add_service(UserServer::new(user))
        .add_service(TopGainerLoserServer::new(top_gainer_loser))
        .serve(addr)
        .await?;

    Ok(())
}
