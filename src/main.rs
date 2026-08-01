use tonic::{Request, Response, Status, transport::Server};

pub mod pb {
    tonic::include_proto!("invezgood");
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
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        let message = request.into_inner().message;
        Ok(Response::new(PingResponse {
            message: format!("pong: {message}"),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("HOST").unwrap_or_else(|_| DEFAULT_HOST.into());
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| DEFAULT_PORT.into());
    let addr = format!("{host}:{port}").parse()?;

    println!("invezgood gRPC listening on {addr}");

    Server::builder()
        .add_service(InvezgoodServer::new(InvezgoodService))
        .serve(addr)
        .await?;

    Ok(())
}
