//! Client / utilitas GCS: service account, signed URL/POST, upload bytes, preview.

use base64::{engine::general_purpose::STANDARD as B64_ENGINE, Engine as _};
use chrono::{Datelike, Duration as ChronoDuration, Utc};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use moka::sync::Cache;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::{pkcs1v15::SigningKey, Pkcs1v15Sign, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

/// Modul path default untuk stoksaham (icon emiten, dll).
pub const MODULE_STOKSAHAM: &str = "stoksaham";

/// Kredensial service account GCS: kunci RSA di-parse sekali.
#[derive(Clone)]
pub struct GcsServiceAccount {
    pub client_email: String,
    signing_key: SigningKey<Sha256>,
    jwt_encoding_key: EncodingKey,
}

impl GcsServiceAccount {
    fn from_private_key_pem(client_email: String, private_key_pem: String) -> Result<Self, Status> {
        let pem = private_key_pem.trim();
        if pem.is_empty() {
            return Err(Status::failed_precondition("GCS private_key kosong"));
        }
        // Normalisasi `\n` literal (sering dari env) → newline sungguhan.
        let pem_normalized = pem.replace("\\n", "\n");
        let pem = pem_normalized.as_str();

        let rsa_key = RsaPrivateKey::from_pkcs8_pem(pem)
            .map_err(|e| Status::failed_precondition(format!("GCS private_key PKCS#8: {e}")))?;
        // jsonwebtoken `from_rsa_der` hanya menerima PKCS#1 DER — pakai PEM (PKCS#8) langsung.
        let jwt_encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|e| {
            Status::failed_precondition(format!("GCS JWT EncodingKey from PEM: {e}"))
        })?;
        let signing_key = SigningKey::<Sha256>::new(rsa_key);
        Ok(Self {
            client_email,
            signing_key,
            jwt_encoding_key,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GcpServiceAccountJson {
    client_email: String,
    private_key: String,
}

pub fn load_gcs_service_account_from_json_path(path: &str) -> Result<GcsServiceAccount, Status> {
    let data = std::fs::read_to_string(path.trim()).map_err(|e| {
        Status::failed_precondition(format!("GCS_SERVICE_ACCOUNT_JSON: gagal baca file: {e}"))
    })?;
    let j: GcpServiceAccountJson = serde_json::from_str(&data).map_err(|e| {
        Status::failed_precondition(format!("GCS_SERVICE_ACCOUNT_JSON: JSON tidak valid: {e}"))
    })?;
    if j.client_email.trim().is_empty() || j.private_key.trim().is_empty() {
        return Err(Status::failed_precondition(
            "service account JSON: client_email atau private_key kosong",
        ));
    }
    GcsServiceAccount::from_private_key_pem(j.client_email, j.private_key)
}

pub fn load_gcs_service_account() -> Result<GcsServiceAccount, Status> {
    let client_email = std::env::var("GCS_SIGNER_CLIENT_EMAIL").unwrap_or_default();
    let private_key = std::env::var("GCS_SIGNER_PRIVATE_KEY").unwrap_or_default();
    if client_email.is_empty() || private_key.is_empty() {
        return Err(Status::failed_precondition(
            "GCS_SIGNER_CLIENT_EMAIL dan GCS_SIGNER_PRIVATE_KEY harus diisi",
        ));
    }
    GcsServiceAccount::from_private_key_pem(client_email, private_key)
}

/// Path file JSON (`GCS_SERVICE_ACCOUNT_JSON`) atau env signer.
pub fn load_gcs_service_account_unified() -> Result<GcsServiceAccount, Status> {
    let path = std::env::var("GCS_SERVICE_ACCOUNT_JSON").unwrap_or_default();
    if !path.trim().is_empty() {
        return load_gcs_service_account_from_json_path(path.trim());
    }
    load_gcs_service_account()
}

fn load_required_positive_i64_env(key: &str) -> Result<i64, String> {
    let v: i64 = std::env::var(key)
        .map_err(|_| format!("{key} harus diatur di environment"))?
        .trim()
        .parse()
        .map_err(|_| format!("{key} harus berupa bilangan bulat yang valid"))?;
    if v <= 0 {
        return Err(format!("{key} harus lebih dari 0"));
    }
    Ok(v)
}

fn load_gcs_signed_url_expires_secs_env() -> Result<u64, String> {
    let v = std::env::var("GCS_SIGNED_URL_EXPIRES_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(3600);
    if !(1..=604_800).contains(&v) {
        return Err("GCS_SIGNED_URL_EXPIRES_SECS harus antara 1 dan 604800 (detik)".into());
    }
    Ok(v)
}

fn load_gcs_preview_url_expires_secs_env(signed_secs: u64) -> Result<u64, String> {
    let _ = signed_secs;
    // Default / maks. cache GeneratePreviewUrl: 7 hari (selaras batas GCS V4 signed URL).
    const SEVEN_DAYS: u64 = 604_800;
    let v = std::env::var("GCS_PREVIEW_URL_EXPIRES_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(SEVEN_DAYS);
    if !(1..=SEVEN_DAYS).contains(&v) {
        return Err(
            "GCS_PREVIEW_URL_EXPIRES_SECS harus antara 1 dan 604800 (7 hari, detik)".into(),
        );
    }
    Ok(v)
}

/// Batas keras Google Cloud Storage V4 signed URL (7 hari).
const GCS_V4_SIGNED_URL_MAX_EXPIRES_SECS: u64 = 604_800;

/// Clamp expires ke batas V4 GCS (max 7 hari) agar URL diterima storage.googleapis.com.
fn clamp_gcs_v4_expires_secs(expires_secs: u64) -> u64 {
    expires_secs.clamp(1, GCS_V4_SIGNED_URL_MAX_EXPIRES_SECS)
}

pub struct GcsSignedUrlRuntime {
    pub bucket: String,
    pub max_foto_size_bytes: i64,
    pub max_video_size_bytes: i64,
    pub signed_url_expires_secs: u64,
    /// Masa berlaku signed GET yang di-sign (≤ 7 hari, batas GCS V4).
    pub preview_url_expires_secs: u64,
    /// TTL cache in-process `GeneratePreviewUrl` per `object_path` (7 hari / 604800).
    pub preview_url_cache_ttl_secs: u64,
    pub account: GcsServiceAccount,
}

pub fn load_gcs_signed_url_runtime() -> Result<GcsSignedUrlRuntime, String> {
    let gcs_bucket = std::env::var("GCS_BUCKET").unwrap_or_default();
    let gcs_bucket = gcs_bucket.trim().to_string();
    if gcs_bucket.is_empty() {
        return Err("GCS_BUCKET harus diatur".into());
    }

    let max_foto = load_required_positive_i64_env("GCS_MAX_FOTO_SIZE_BYTES")?;
    let max_video = load_required_positive_i64_env("GCS_MAX_VIDEO_SIZE_BYTES")?;
    let signed = load_gcs_signed_url_expires_secs_env()?;
    let cache_ttl = load_gcs_preview_url_expires_secs_env(signed)?;
    // Signed GET per URL: clamp ke max V4; cache entry bisa hidup sampai cache_ttl.
    let preview_sign = clamp_gcs_v4_expires_secs(cache_ttl);
    let account = load_gcs_service_account_unified().map_err(|e| e.message().to_string())?;

    Ok(GcsSignedUrlRuntime {
        bucket: gcs_bucket,
        max_foto_size_bytes: max_foto,
        max_video_size_bytes: max_video,
        signed_url_expires_secs: signed,
        preview_url_expires_secs: preview_sign,
        preview_url_cache_ttl_secs: cache_ttl,
        account,
    })
}

fn q_component(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

fn gcs_canonical_uri(bucket: &str, object_in_bucket: &str) -> String {
    let b = bucket.trim();
    let o = object_in_bucket.trim().trim_start_matches('/');
    let mut segs = vec![q_component(b)];
    for p in o.split('/').filter(|x| !x.is_empty()) {
        segs.push(q_component(p));
    }
    format!("/{}", segs.join("/"))
}

pub fn gcs_v4_sign_url_with_query(
    account: &GcsServiceAccount,
    bucket: &str,
    object_in_bucket: &str,
    expires_secs: u64,
    http_method: &str,
    extra_query: &[(&str, String)],
) -> Result<(String, i64), Status> {
    let expires_secs = clamp_gcs_v4_expires_secs(expires_secs);
    let now = Utc::now();
    let x_goog_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let credential_scope = format!("{}/auto/storage/goog4_request", datestamp);
    let credential = format!("{}/{}", account.client_email.trim(), credential_scope);

    let canonical_uri = gcs_canonical_uri(bucket, object_in_bucket);

    let mut qpairs: Vec<(&str, String)> = vec![
        ("X-Goog-Algorithm", "GOOG4-RSA-SHA256".to_string()),
        ("X-Goog-Credential", credential),
        ("X-Goog-Date", x_goog_date.clone()),
        ("X-Goog-Expires", expires_secs.to_string()),
        ("X-Goog-SignedHeaders", "host".to_string()),
    ];
    qpairs.extend(extra_query.iter().map(|(k, v)| (*k, v.clone())));
    qpairs.sort_by(|a, b| a.0.cmp(b.0));

    let canonical_qs: String = qpairs
        .iter()
        .map(|(k, v)| format!("{}={}", q_component(k), q_component(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = "host:storage.googleapis.com\n";
    let signed_headers = "host";
    let payload = "UNSIGNED-PAYLOAD";

    let canonical_request = format!(
        "{http_method}\n{canonical_uri}\n{canonical_qs}\n{canonical_headers}\n{signed_headers}\n{payload}"
    );

    let mut h = Sha256::new();
    h.update(canonical_request.as_bytes());
    let cr_hash = hex::encode(h.finalize());

    let string_to_sign = format!("GOOG4-RSA-SHA256\n{x_goog_date}\n{credential_scope}\n{cr_hash}");

    let signature = account
        .signing_key
        .try_sign(string_to_sign.as_bytes())
        .map_err(|e| Status::internal(format!("GCS V4 RSA sign: {e}")))?;
    let hex_sig = hex::encode(signature.to_bytes());

    let expires_at_unix = now.timestamp() + expires_secs as i64;

    let full_url = format!(
        "https://storage.googleapis.com{}?{}&X-Goog-Signature={}",
        canonical_uri, canonical_qs, hex_sig
    );
    Ok((full_url, expires_at_unix))
}

pub fn gcs_v4_sign_url(
    account: &GcsServiceAccount,
    bucket: &str,
    object_in_bucket: &str,
    expires_secs: u64,
    http_method: &str,
) -> Result<(String, i64), Status> {
    gcs_v4_sign_url_with_query(
        account,
        bucket,
        object_in_bucket,
        expires_secs,
        http_method,
        &[],
    )
}

pub fn gcs_v4_sign_get_url(
    account: &GcsServiceAccount,
    bucket: &str,
    object_in_bucket: &str,
    expires_secs: u64,
) -> Result<(String, i64), Status> {
    gcs_v4_sign_url(account, bucket, object_in_bucket, expires_secs, "GET")
}

#[derive(Serialize)]
struct GcsPostPolicyDocument {
    conditions: Vec<serde_json::Value>,
    expiration: String,
}

pub fn gcs_v4_signed_post_policy_v4(
    account: &GcsServiceAccount,
    bucket: &str,
    object_in_bucket: &str,
    content_type: &str,
    max_bytes: u64,
    expires_secs: u64,
) -> Result<(String, HashMap<String, String>, i64), Status> {
    let bucket = bucket.trim();
    let object = object_in_bucket.trim().trim_start_matches('/');
    if bucket.is_empty() || object.is_empty() {
        return Err(Status::invalid_argument("bucket dan object wajib diisi"));
    }
    if max_bytes == 0 {
        return Err(Status::invalid_argument("max_bytes harus lebih dari 0"));
    }
    let expires_secs = clamp_gcs_v4_expires_secs(expires_secs);
    let ct = content_type.trim();
    if ct.is_empty() {
        return Err(Status::invalid_argument("content_type wajib diisi"));
    }

    let now = Utc::now();
    let expires_at = now
        .checked_add_signed(ChronoDuration::seconds(expires_secs as i64))
        .ok_or_else(|| Status::invalid_argument("expires_secs di luar rentang yang valid"))?;
    if expires_at <= now {
        return Err(Status::invalid_argument("expires_secs harus > 0"));
    }
    let expires_at_unix = expires_at.timestamp();

    let x_goog_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let credential = format!(
        "{}/{}/auto/storage/goog4_request",
        account.client_email.trim(),
        datestamp
    );

    let mut conditions: Vec<serde_json::Value> = Vec::new();
    conditions.push(json!(["content-length-range", 0, max_bytes]));
    conditions.push(json!({ "content-type": ct }));
    conditions.push(json!({ "success_action_status": "204" }));
    conditions.push(json!({ "bucket": bucket }));
    conditions.push(json!({ "key": object }));
    conditions.push(json!({ "x-goog-date": &x_goog_date }));
    conditions.push(json!({ "x-goog-credential": &credential }));
    conditions.push(json!({ "x-goog-algorithm": "GOOG4-RSA-SHA256" }));

    let doc = GcsPostPolicyDocument {
        conditions,
        expiration: expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    let policy_json = serde_json::to_string(&doc)
        .map_err(|e| Status::internal(format!("GCS policy JSON: {e}")))?;
    let b64_policy = B64_ENGINE.encode(policy_json.as_bytes());

    let digest = Sha256::digest(b64_policy.as_bytes());
    let sig_bytes = account
        .signing_key
        .as_ref()
        .sign_with_rng(&mut rand::rngs::OsRng, Pkcs1v15Sign::new::<Sha256>(), digest.as_slice())
        .map_err(|e| Status::internal(format!("GCS POST policy RSA sign: {e}")))?;
    let hex_sig = hex::encode(sig_bytes);

    let post_url = format!("https://storage.googleapis.com/{}/", bucket);

    let mut fields = HashMap::new();
    fields.insert("key".to_string(), object.to_string());
    fields.insert("x-goog-date".to_string(), x_goog_date);
    fields.insert("x-goog-credential".to_string(), credential);
    fields.insert(
        "x-goog-algorithm".to_string(),
        "GOOG4-RSA-SHA256".to_string(),
    );
    fields.insert("content-type".to_string(), ct.to_string());
    fields.insert("success_action_status".to_string(), "204".to_string());
    fields.insert("policy".to_string(), b64_policy);
    fields.insert("x-goog-signature".to_string(), hex_sig);
    fields.retain(|_, v| !v.is_empty());

    Ok((post_url, fields, expires_at_unix))
}

#[derive(Debug, Serialize)]
struct GcpJwtClaims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

#[derive(Clone, Debug)]
pub struct GcsOAuthAccessTokenSnapshot {
    pub access_token: String,
    pub valid_until_unix: i64,
}

const GCS_OAUTH_REFRESH_MARGIN_SECS: i64 = 300;

#[derive(Clone)]
pub struct GcsOAuthTokenCache {
    snapshot: Arc<RwLock<Option<GcsOAuthAccessTokenSnapshot>>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
}

impl GcsOAuthTokenCache {
    pub fn new() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(None)),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn access_token(&self, account: &GcsServiceAccount) -> Result<String, Status> {
        let mut now = unix_now_secs()?;
        {
            let guard = self.snapshot.read().await;
            if let Some(ref snap) = *guard {
                if snap.valid_until_unix > now {
                    return Ok(snap.access_token.clone());
                }
            }
        }

        let _refresh = self.refresh_lock.lock().await;
        now = unix_now_secs()?;
        {
            let guard = self.snapshot.read().await;
            if let Some(ref snap) = *guard {
                if snap.valid_until_unix > now {
                    return Ok(snap.access_token.clone());
                }
            }
        }

        let fresh = fetch_gcs_oauth_access_token_async(account).await?;
        let mut guard = self.snapshot.write().await;
        if let Some(ref snap) = *guard {
            if snap.valid_until_unix > now {
                return Ok(snap.access_token.clone());
            }
        }
        let token = fresh.access_token.clone();
        *guard = Some(fresh);
        Ok(token)
    }
}

impl Default for GcsOAuthTokenCache {
    fn default() -> Self {
        Self::new()
    }
}

fn unix_now_secs() -> Result<i64, Status> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time error"))?
        .as_secs() as i64)
}

pub async fn fetch_gcs_oauth_access_token_async(
    account: &GcsServiceAccount,
) -> Result<GcsOAuthAccessTokenSnapshot, Status> {
    let now = unix_now_secs()?;
    let claims = GcpJwtClaims {
        iss: account.client_email.clone(),
        scope: "https://www.googleapis.com/auth/devstorage.full_control".to_string(),
        aud: "https://oauth2.googleapis.com/token".to_string(),
        iat: now,
        exp: now + 3600,
    };
    let header = Header::new(Algorithm::RS256);
    let jwt = encode(&header, &claims, &account.jwt_encoding_key)
        .map_err(|e| Status::internal(format!("JWT encode error: {e}")))?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ])
        .send()
        .await
        .map_err(|e| Status::internal(format!("OAuth2 token request failed: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Status::internal(format!(
            "OAuth2 token error {status}: {body}"
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| Status::internal(format!("OAuth2 token parse error: {e}")))?;
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Status::internal("OAuth2 response missing access_token"))?;
    let expires_in = json
        .get("expires_in")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
        })
        .filter(|&e| e > 0)
        .unwrap_or(3600);
    let margin = GCS_OAUTH_REFRESH_MARGIN_SECS.min(expires_in.saturating_sub(1).max(1));
    Ok(GcsOAuthAccessTokenSnapshot {
        access_token: access_token.to_string(),
        valid_until_unix: now + expires_in - margin,
    })
}

/// Path objek baru: `{nama_modul}/{tahun}/{tahun-bulan}/{cabang}/{uuid_v7}.{ext}`.
pub fn gcs_new_object_path(nama_modul: &str, cabang: &str, ext: &str) -> String {
    let id = Uuid::now_v7();
    let now = Utc::now();
    let tahun_bulan = now.format("%Y-%m").to_string();
    format!(
        "{}/{}/{}/{}/{}.{}",
        nama_modul.trim(),
        now.year(),
        tahun_bulan,
        cabang.trim(),
        id,
        ext
    )
}

/// Path stabil icon emiten: `stoksaham/icon/{EMITEN}.{ext}`.
pub fn emiten_icon_object_path(emiten: &str, ext: &str) -> String {
    let code = emiten.trim().to_ascii_uppercase();
    let ext = ext.trim().trim_start_matches('.').to_ascii_lowercase();
    format!("{MODULE_STOKSAHAM}/icon/{code}.{ext}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaKind {
    Photo,
    Video,
    Audio,
    Document,
}

pub fn gcs_upload_extension(ext: &str) -> Result<(&'static str, MediaKind, &'static str), Status> {
    let e = ext.trim().trim_start_matches('.').to_lowercase();
    let e = if e.is_empty() { "jpg".to_string() } else { e };
    if e.len() > 8 || !e.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(Status::invalid_argument("file_extension tidak valid"));
    }
    match e.as_str() {
        "jpg" | "jpeg" => Ok(("jpg", MediaKind::Photo, "image/jpeg")),
        "png" => Ok(("png", MediaKind::Photo, "image/png")),
        "webp" => Ok(("webp", MediaKind::Photo, "image/webp")),
        "heic" => Ok(("heic", MediaKind::Photo, "image/heic")),
        "svg" => Ok(("svg", MediaKind::Photo, "image/svg+xml")),
        "mp4" => Ok(("mp4", MediaKind::Video, "video/mp4")),
        "webm" => Ok(("webm", MediaKind::Video, "video/webm")),
        "mov" => Ok(("mov", MediaKind::Video, "video/quicktime")),
        "m4v" => Ok(("m4v", MediaKind::Video, "video/x-m4v")),
        "mp3" => Ok(("mp3", MediaKind::Audio, "audio/mpeg")),
        "ogg" => Ok(("ogg", MediaKind::Audio, "audio/ogg")),
        "aac" | "acc" => Ok(("aac", MediaKind::Audio, "audio/aac")),
        "3gp" => Ok(("3gp", MediaKind::Audio, "audio/3gpp")),
        "alac" => Ok(("alac", MediaKind::Audio, "audio/alac")),
        "m4a" => Ok(("m4a", MediaKind::Audio, "audio/mp4")),
        "pdf" => Ok(("pdf", MediaKind::Document, "application/pdf")),
        _ => Err(Status::invalid_argument(
            "file yang boleh: foto (jpg, png, heic, webp, svg), video, audio, dokumen (pdf)",
        )),
    }
}

pub fn gcs_object_extension_from_path(object_path: &str) -> Option<String> {
    let path = object_path.split('?').next()?.trim();
    let ext = path.rsplit('.').next()?.trim().to_lowercase();
    if ext.is_empty() || ext == path.to_lowercase() {
        None
    } else {
        Some(ext)
    }
}

pub fn gcs_object_content_type_from_path(object_path: &str) -> Option<&'static str> {
    let ext = gcs_object_extension_from_path(object_path)?;
    gcs_upload_extension(ext.as_str()).ok().map(|(_, _, ct)| ct)
}

pub fn gcs_object_path_preview(path: &str) -> Result<String, Status> {
    let p = path.trim().trim_start_matches('/');
    if p.is_empty() {
        return Err(Status::invalid_argument("object_path tidak boleh kosong"));
    }
    if p.contains("..") {
        return Err(Status::invalid_argument("object_path tidak valid"));
    }
    Ok(p.to_string())
}

pub fn gcs_object_wire_path(bucket: &str, foto_path: &str) -> Option<String> {
    let p = foto_path.trim();
    if p.is_empty() || p.contains("..") {
        return None;
    }
    let bucket = bucket.trim().trim_end_matches('/');
    let key = if let Some(rest) = p.strip_prefix("gs://") {
        let rest = rest.trim_start_matches('/');
        let slash = rest.find('/')?;
        if &rest[..slash] != bucket {
            return None;
        }
        rest[slash + 1..].trim_start_matches('/')
    } else {
        p.trim_start_matches('/')
    };
    if key.is_empty() {
        return None;
    }
    Some(key.to_string())
}

pub fn gcs_signed_get_url_with_response_content_type(
    bucket: &str,
    object_path_wire: &str,
    expires_secs: u64,
    account: &GcsServiceAccount,
    content_type: &str,
) -> Result<(String, i64), Status> {
    let object_in_bucket = gcs_object_path_preview(object_path_wire)?;
    let extra = [("response-content-type", content_type.to_string())];
    gcs_v4_sign_url_with_query(
        account,
        bucket,
        &object_in_bucket,
        expires_secs,
        "GET",
        &extra,
    )
}

pub fn gcs_signed_get_url_for_stored_object_preview(
    bucket: &str,
    object_path_wire: &str,
    expires_secs: u64,
    account: &GcsServiceAccount,
) -> Result<(String, i64), Status> {
    if let Some(content_type) = gcs_object_content_type_from_path(object_path_wire) {
        return gcs_signed_get_url_with_response_content_type(
            bucket,
            object_path_wire,
            expires_secs,
            account,
            content_type,
        );
    }
    let object_in_bucket = gcs_object_path_preview(object_path_wire)?;
    gcs_v4_sign_get_url(account, bucket, &object_in_bucket, expires_secs)
}

#[derive(Debug)]
pub struct GcsSignedPostUploadOutcome {
    pub post_url: String,
    pub object_path: String,
    pub content_type: String,
    pub expires_at_unix: i64,
    pub max_file_size_bytes: i64,
    pub post_form_fields: HashMap<String, String>,
}

pub fn gcs_signed_post_upload(
    bucket: &str,
    file_extension: &str,
    max_foto_size_bytes: i64,
    max_video_size_bytes: i64,
    expires_secs: u64,
    account: &GcsServiceAccount,
    object_path: String,
) -> Result<GcsSignedPostUploadOutcome, Status> {
    let (ext, media_kind, content_type) = gcs_upload_extension(file_extension)?;
    let _ = ext;
    let max_file_size_bytes: i64 = match media_kind {
        MediaKind::Photo => max_foto_size_bytes,
        MediaKind::Video | MediaKind::Audio | MediaKind::Document => max_video_size_bytes,
    };
    let max_bytes_u64 = u64::try_from(max_file_size_bytes).map_err(|_| {
        Status::failed_precondition("batas ukuran file terlalu besar untuk policy GCS")
    })?;

    let (post_url, post_form_fields, expires_at_unix) = gcs_v4_signed_post_policy_v4(
        account,
        bucket,
        object_path.as_str(),
        content_type,
        max_bytes_u64,
        expires_secs,
    )?;

    Ok(GcsSignedPostUploadOutcome {
        post_url,
        object_path,
        content_type: content_type.to_string(),
        expires_at_unix,
        max_file_size_bytes,
        post_form_fields,
    })
}

/// Upload bytes ke GCS via JSON API (OAuth). Mengembalikan `object_path` relatif bucket.
pub async fn gcs_upload_bytes(
    bucket: &str,
    object_path: &str,
    bytes: &[u8],
    content_type: &str,
    account: &GcsServiceAccount,
    oauth_cache: &GcsOAuthTokenCache,
) -> Result<String, Status> {
    let object = object_path.trim().trim_start_matches('/');
    if bucket.trim().is_empty() || object.is_empty() {
        return Err(Status::invalid_argument("bucket dan object_path wajib"));
    }
    let token = oauth_cache.access_token(account).await?;
    let name_q = urlencoding::encode(object);
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket.trim(),
        name_q
    );
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(&token)
        .header("Content-Type", content_type)
        .body(bytes.to_vec())
        .send()
        .await
        .map_err(|e| Status::internal(format!("GCS upload request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Status::internal(format!(
            "GCS upload error {status}: {body}"
        )));
    }
    Ok(object.to_string())
}

/// Hapus object via JSON API.
pub async fn gcs_delete_object(
    bucket: &str,
    object_path: &str,
    account: &GcsServiceAccount,
    oauth_cache: &GcsOAuthTokenCache,
) -> Result<(), Status> {
    let object = gcs_object_wire_path(bucket, object_path)
        .ok_or_else(|| Status::invalid_argument("path object tidak valid"))?;
    let token = oauth_cache.access_token(account).await?;
    let name_q = urlencoding::encode(&object);
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        bucket.trim(),
        name_q
    );
    let client = reqwest::Client::new();
    let resp = client
        .delete(&url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| Status::internal(format!("GCS delete request failed: {e}")))?;
    if resp.status().as_u16() == 404 {
        return Ok(());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Status::internal(format!(
            "GCS delete error {status}: {body}"
        )));
    }
    Ok(())
}

/// Download dari URL sumber lalu upload ke GCS sebagai icon emiten.
/// Mengembalikan path object relatif (`stoksaham/icon/{CODE}.ext`) untuk disimpan di DB.
pub async fn download_and_upload_emiten_icon(
    emiten: &str,
    source_url: &str,
    runtime: &GcsSignedUrlRuntime,
    oauth_cache: &GcsOAuthTokenCache,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if source_url.trim().is_empty() {
        return Ok(String::new());
    }
    let path = source_url.split('?').next().unwrap_or(source_url);
    let ext_raw = path
        .rsplit('.')
        .next()
        .filter(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "webp" | "svg"
            )
        })
        .unwrap_or("png");
    let (ext, _, content_type) = gcs_upload_extension(ext_raw)
        .map_err(|e| e.message().to_string())?;
    let object_path = emiten_icon_object_path(emiten, ext);
    let bytes = reqwest::get(source_url)
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    gcs_upload_bytes(
        runtime.bucket.as_str(),
        &object_path,
        &bytes,
        content_type,
        &runtime.account,
        oauth_cache,
    )
    .await
    .map_err(|e| e.message().to_string())?;
    Ok(object_path)
}

/// Cache signed preview URL per object_path (TTL 7 hari).
pub struct PreviewUrlCache {
    inner: Cache<String, (String, i64)>,
}

impl PreviewUrlCache {
    pub fn new(ttl_secs: u64) -> Self {
        let ttl = Duration::from_secs(ttl_secs.max(1));
        Self {
            inner: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(10_000)
                .build(),
        }
    }

    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Cache hit bila `expires_at_unix` masih > now; bila URL kedaluwarsa → re-sign.
    /// Return ketiga: `true` bila diambil dari cache.
    pub fn get_or_load<F>(&self, object_path: &str, load: F) -> Result<(String, i64, bool), Status>
    where
        F: FnOnce() -> Result<(String, i64), Status>,
    {
        let key = object_path.trim().to_string();
        let now = Self::unix_now();
        if let Some(v) = self.inner.get(&key) {
            if v.1 > now {
                return Ok((v.0, v.1, true));
            }
            self.inner.invalidate(&key);
        }
        let v = load()?;
        self.inner.insert(key, v.clone());
        Ok((v.0, v.1, false))
    }
}

static RUNTIME: OnceLock<Result<GcsSignedUrlRuntime, String>> = OnceLock::new();

/// Runtime GCS global (lazy dari env).
pub fn gcs_runtime() -> Result<&'static GcsSignedUrlRuntime, Status> {
    match RUNTIME.get_or_init(load_gcs_signed_url_runtime) {
        Ok(r) => Ok(r),
        Err(e) => Err(Status::failed_precondition(e.clone())),
    }
}
