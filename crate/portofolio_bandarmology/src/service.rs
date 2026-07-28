use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::model::PortofolioBandarmology;
use crate::portofolio_bandarmology_server::PortofolioBandarmology as PortofolioBandarmologyRpc;
use crate::repository::PortofolioBandarmologyRepository;
use crate::{GetPortofolioBandarmologyRequest, GetPortofolioBandarmologyResponse};

fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet (contoh: BBCA)".into());
    }
    Ok(kode)
}

pub struct PortofolioBandarmologyService {
    repo: PortofolioBandarmologyRepository,
}

impl PortofolioBandarmologyService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: PortofolioBandarmologyRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl PortofolioBandarmologyRpc for PortofolioBandarmologyService {
    async fn get_portofolio_bandarmology(
        &self,
        request: Request<GetPortofolioBandarmologyRequest>,
    ) -> Result<Response<GetPortofolioBandarmologyResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = parse_emiten_name(&req.emiten_name).map_err(Status::invalid_argument)?;

        let rows = self
            .repo
            .find_by_emiten(&kode)
            .await
            .map_err(|e| Status::internal(format!("Scylla query failed: {e}")))?;

        if rows.is_empty() {
            println!(
                "GetPortofolioBandarmology {} {kode} NOT_FOUND {}ms",
                username,
                started.elapsed().as_millis()
            );
            return Err(Status::not_found(format!(
                "portofolio_bandarmology emiten_name={kode} tidak ditemukan"
            )));
        }

        let proto_rows: Vec<_> = rows
            .into_iter()
            .map(PortofolioBandarmology::into_proto)
            .collect();

        println!(
            "GetPortofolioBandarmology {} {kode} rows={} {}ms",
            username,
            proto_rows.len(),
            started.elapsed().as_millis()
        );

        Ok(Response::new(GetPortofolioBandarmologyResponse {
            rows: proto_rows,
        }))
    }
}
