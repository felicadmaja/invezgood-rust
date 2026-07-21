//! Helpers JWT untuk crate lain (portofolio, dll).

use chrono::{Local, Timelike};
use tonic::{Request, Status};

use crate::jwt::{decode_token, Claims};

/// Ambil Bearer token dari metadata `authorization`.
fn bearer_token<T>(req: &Request<T>) -> Result<&str, Status> {
    let value = req
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("Authorization Bearer token wajib"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("Authorization header tidak valid"))?;

    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .ok_or_else(|| Status::unauthenticated("format: Authorization: Bearer <token>"))?
        .trim();

    if token.is_empty() {
        return Err(Status::unauthenticated("token kosong"));
    }
    Ok(token)
}

/// Validasi JWT dan masukkan [`Claims`] ke request extensions.
/// Path Login (`user.User/Login`) dilewati.
#[derive(Clone, Default)]
pub struct AuthInterceptor;

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        // Beberapa path Login: jangan paksa JWT.
        if let Some(path) = request
            .metadata()
            .get(":path")
            .or_else(|| request.metadata().get("path"))
            .and_then(|v| v.to_str().ok())
        {
            if path.contains("user.User/Login") || path.ends_with("/Login") {
                return Ok(request);
            }
        }

        let token = bearer_token(&request)?;
        let claims = decode_token(token)
            .map_err(|e| Status::unauthenticated(format!("JWT tidak valid: {e}")))?;
        request.extensions_mut().insert(claims);
        Ok(request)
    }
}

/// Wajib login: ambil [`Claims`] dari extensions (setelah interceptor).
pub fn take_claims<T>(request: &Request<T>) -> Result<&Claims, Status> {
    request
        .extensions()
        .get::<Claims>()
        .ok_or_else(|| Status::unauthenticated("harus login dulu (JWT tidak ada)"))
}

/// Validasi JWT langsung di handler (tanpa interceptor).
pub fn require_auth<T>(request: &Request<T>) -> Result<Claims, Status> {
    if let Some(claims) = request.extensions().get::<Claims>() {
        return Ok(claims.clone());
    }
    let token = bearer_token(request)?;
    decode_token(token).map_err(|e| Status::unauthenticated(format!("JWT tidak valid: {e}")))
}

/// Batasi RPC scrape Stockbit on-demand ke jam server lokal 07:00–17:00
/// (inklusif 07:00, eksklusif 17:00). Di luar itu → `FAILED_PRECONDITION`.
/// Tidak dipakai oleh bin `worker_scrapping` (cron/manual).
pub fn require_stockbit_scrape_hours() -> Result<(), Status> {
    let now = Local::now();
    let mins = now.hour() * 60 + now.minute();
    const START_MINS: u32 = 7 * 60; // 07:00
    const END_MINS: u32 = 17 * 60; // 17:00
    if mins < START_MINS || mins >= END_MINS {
        return Err(Status::failed_precondition("Diluar jam 07:00 - 17:00"));
    }
    Ok(())
}
