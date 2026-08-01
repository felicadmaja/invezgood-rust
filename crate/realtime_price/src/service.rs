use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use user::require_admin;

use crate::hub::RealtimePriceHub;
use crate::realtime_price_server::RealtimePrice as RealtimePriceRpc;
use crate::{GetRealtimePriceFromStockbitRequest, GetRealtimePriceFromStockbitResponse};

fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let code = raw.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name wajib tepat 4 huruf alphabet (contoh CUAN)".into());
    }
    Ok(code)
}

pub struct RealtimePriceService {
    hub: Arc<RealtimePriceHub>,
}

impl RealtimePriceService {
    pub fn new() -> Self {
        Self {
            hub: Arc::new(RealtimePriceHub::new()),
        }
    }
}

impl Default for RealtimePriceService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl RealtimePriceRpc for RealtimePriceService {
    type GetRealtimePriceFromStockbitStream =
        Pin<Box<dyn Stream<Item = Result<GetRealtimePriceFromStockbitResponse, Status>> + Send>>;

    async fn get_realtime_price_from_stockbit(
        &self,
        request: Request<GetRealtimePriceFromStockbitRequest>,
    ) -> Result<Response<Self::GetRealtimePriceFromStockbitStream>, Status> {
        let started = Instant::now();
        let claims = require_admin(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let code = parse_emiten_name(&req.emiten_name).map_err(Status::invalid_argument)?;

        println!(
            "GetRealtimePriceFromStockbit: client subscribe user={username} emiten_name={code}"
        );

        let mut sub = self.hub.subscribe(code.clone()).await;
        let (tx, rx) = mpsc::channel::<Result<GetRealtimePriceFromStockbitResponse, Status>>(8);

        println!(
            "GetRealtimePriceFromStockbit: stream aktif user={username} emiten_name={code} {}ms",
            started.elapsed().as_millis()
        );

        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                if tx.send(Ok(msg)).await.is_err() {
                    println!(
                        "GetRealtimePriceFromStockbit: client unsubscribe/disconnect user={username} emiten_name={code}"
                    );
                    break;
                }
            }
            // `sub` drop → kurangi subscriber; poller berhenti bila 0.
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
