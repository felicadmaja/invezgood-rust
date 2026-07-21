use std::sync::Arc;
use std::time::Instant;

use chrono::Local;
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;
use worker_scrapping::portofolio_bandarmology_worker;

use crate::database::keyspace;
use crate::model::PortofolioBandarmology;
use crate::portofolio_bandarmology_server::PortofolioBandarmology as PortofolioBandarmologyRpc;
use crate::repository::PortofolioBandarmologyRepository;
use crate::{
    InsertPortofolioBandarmologyRequest, InsertPortofolioBandarmologyResponse,
};

fn parse_emiten_name(raw: &str) -> Result<String, String> {
    let kode = raw.trim().to_ascii_uppercase();
    if kode.len() != 4 || !kode.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet (contoh: BBCA)".into());
    }
    Ok(kode)
}

pub struct PortofolioBandarmologyService {
    repo: PortofolioBandarmologyRepository,
    session: Arc<Session>,
}

impl PortofolioBandarmologyService {
    pub fn new(session: Arc<Session>) -> Self {
        let session_for_repo = session.clone();
        Self {
            repo: PortofolioBandarmologyRepository::new(session_for_repo),
            session,
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl PortofolioBandarmologyRpc for PortofolioBandarmologyService {
    async fn insert_portofolio_bandarmology(
        &self,
        request: Request<InsertPortofolioBandarmologyRequest>,
    ) -> Result<Response<InsertPortofolioBandarmologyResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let kode = match parse_emiten_name(&req.emiten_name) {
            Ok(c) => c,
            Err(message) => {
                return Ok(Response::new(InsertPortofolioBandarmologyResponse {
                    success: false,
                    message,
                    row: None,
                }));
            }
        };

        let ks = keyspace();
        println!("InsertPortofolioBandarmology {username}: {kode}...");

        match portofolio_bandarmology_worker::insert_portofolio_bandarmology_for_emiten(
            self.session.as_ref(),
            &ks,
            &kode,
        )
        .await
        {
            Ok(true) => {
                let today = Local::now().date_naive();
                let row = self
                    .repo
                    .find_by_emiten_and_date(&kode, today)
                    .await
                    .ok()
                    .flatten()
                    .map(PortofolioBandarmology::into_proto);
                let message = format!(
                    "portofolio_bandarmology {kode}: upsert {today} OK"
                );
                println!(
                    "InsertPortofolioBandarmology {} {kode} success=true {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(InsertPortofolioBandarmologyResponse {
                    success: true,
                    message,
                    row,
                }))
            }
            Ok(false) => {
                let message = format!(
                    "portofolio_bandarmology {kode}: sumber bandarmology minggu berjalan kosong / tidak ada"
                );
                println!(
                    "InsertPortofolioBandarmology {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(InsertPortofolioBandarmologyResponse {
                    success: false,
                    message,
                    row: None,
                }))
            }
            Err(e) => {
                eprintln!("InsertPortofolioBandarmology {username}: gagal {kode}: {e}");
                println!(
                    "InsertPortofolioBandarmology {} {kode} success=false {}ms",
                    username,
                    started.elapsed().as_millis()
                );
                Ok(Response::new(InsertPortofolioBandarmologyResponse {
                    success: false,
                    message: format!("insert portofolio_bandarmology gagal: {e}"),
                    row: None,
                }))
            }
        }
    }
}
