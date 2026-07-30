//! Helpers JWT untuk crate lain (portofolio, dll).

use chrono::{Datelike, Local, Timelike, Weekday};
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

/// Wajib JWT + role `admin` (case-insensitive). Selain itu → `PERMISSION_DENIED` "Harus admin !".
pub fn require_admin<T>(request: &Request<T>) -> Result<Claims, Status> {
    let claims = require_auth(request)?;
    if claims.role.trim().eq_ignore_ascii_case("admin") {
        Ok(claims)
    } else {
        Err(Status::permission_denied("Harus admin !"))
    }
}

/// Batasi RPC scrape Stockbit on-demand ke **hari operasional Senin–Jumat**
/// dan jam server lokal **08:45–12:15** serta **13:25–16:15**
/// (inklusif awal, eksklusif akhir tiap jendela — 12:15 dan 16:15 masih dalam jendela).
/// Sabtu/Minggu atau di luar jam → `FAILED_PRECONDITION`.
/// Tidak dipakai oleh bin `worker_scrapping` (cron/manual).
/// Dipakai juga auto-scrape dari `IsStockbitReady`.
pub fn require_stockbit_scrape_hours() -> Result<(), Status> {
    let now = Local::now();
    match now.weekday() {
        Weekday::Sat | Weekday::Sun => {
            return Err(Status::failed_precondition(
                "Diluar hari operasional Senin-Jumat (Sabtu/Minggu tidak scrape)",
            ));
        }
        Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri => {}
    }

    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 8 * 60 + 45; // 08:45
    const MORNING_END: u32 = 12 * 60 + 16; // setelah 12:15
    const AFTERNOON_START: u32 = 13 * 60 + 25; // 13:25
    const AFTERNOON_END: u32 = 16 * 60 + 16; // setelah 16:15
    let in_morning = mins >= MORNING_START && mins < MORNING_END;
    let in_afternoon = mins >= AFTERNOON_START && mins < AFTERNOON_END;
    if !in_morning && !in_afternoon {
        return Err(Status::failed_precondition(
            "Diluar jam 08:45-12:15 dan 13:25-16:15",
        ));
    }
    Ok(())
}
