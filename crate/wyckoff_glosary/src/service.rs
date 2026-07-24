use std::sync::Arc;
use std::time::Instant;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::require_auth;

use crate::repository::WyckoffGlossaryRepository;
use crate::wyckoff_glossary_server::WyckoffGlossary as WyckoffGlossaryRpc;
use crate::{InsertWyckoffGlossaryRequest, InsertWyckoffGlossaryResponse};

pub struct WyckoffGlossaryService {
    repo: WyckoffGlossaryRepository,
}

impl WyckoffGlossaryService {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            repo: WyckoffGlossaryRepository::new(session),
        }
    }

    pub async fn warm_prepared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.repo.warm_prepared().await
    }
}

#[tonic::async_trait]
impl WyckoffGlossaryRpc for WyckoffGlossaryService {
    async fn insert_wyckoff_glossary(
        &self,
        request: Request<InsertWyckoffGlossaryRequest>,
    ) -> Result<Response<InsertWyckoffGlossaryResponse>, Status> {
        let started = Instant::now();
        let claims = require_auth(&request)?;
        let username = claims.name.clone();
        let req = request.into_inner();

        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Ok(Response::new(InsertWyckoffGlossaryResponse {
                success: false,
                message: "name wajib diisi".to_string(),
            }));
        }

        let description = req.description.trim().to_string();

        let inserted = self
            .repo
            .insert(&name, &description)
            .await
            .map_err(|e| Status::internal(format!("Scylla insert failed: {e}")))?;

        let (success, message) = if inserted {
            (true, format!("wyckoff_glossary name={name} berhasil diinsert"))
        } else {
            (false, format!("wyckoff_glossary name={name} sudah ada"))
        };

        println!(
            "InsertWyckoffGlossary {} {name} success={success} {}ms",
            username,
            started.elapsed().as_millis()
        );

        Ok(Response::new(InsertWyckoffGlossaryResponse {
            success,
            message,
        }))
    }
}
