//! gRPC **`Gcs`** — signed upload/preview URL dan hapus object GCS.

use tonic::{Request, Response, Status};
use user::require_auth;

use crate::client::{
    gcs_delete_object, gcs_new_object_path, gcs_object_wire_path,
    gcs_signed_get_url_for_stored_object_preview, gcs_signed_post_upload, gcs_upload_extension,
    GcsOAuthTokenCache, GcsSignedUrlRuntime, PreviewUrlCache, MODULE_STOKSAHAM,
};
use crate::gcs_server::Gcs as GcsRpc;
use crate::{
    GcsDeleteObjectRequest, GcsDeleteObjectResponse, GeneratePreviewUrlRequest,
    GeneratePreviewUrlResponse, GetStorageUrlRequest, GetStorageUrlResponse,
};

#[derive(Clone)]
pub struct GcsGrpcService {
    runtime: std::sync::Arc<GcsSignedUrlRuntime>,
    oauth: GcsOAuthTokenCache,
    preview_cache: std::sync::Arc<PreviewUrlCache>,
}

impl GcsGrpcService {
    pub fn new(runtime: GcsSignedUrlRuntime) -> Self {
        let preview_ttl = runtime.preview_url_cache_ttl_secs;
        Self {
            runtime: std::sync::Arc::new(runtime),
            oauth: GcsOAuthTokenCache::new(),
            preview_cache: std::sync::Arc::new(PreviewUrlCache::new(preview_ttl)),
        }
    }

    /// Muat runtime dari env (GCS_BUCKET, credentials, …).
    pub fn from_env() -> Result<Self, String> {
        Ok(Self::new(crate::client::load_gcs_signed_url_runtime()?))
    }

    pub fn runtime(&self) -> &GcsSignedUrlRuntime {
        &self.runtime
    }

    pub fn oauth_cache(&self) -> &GcsOAuthTokenCache {
        &self.oauth
    }

    /// Signed GET preview tanpa auth — pemanggil internal sudah auth sendiri.
    pub fn generate_preview_url_for_object_path(
        &self,
        object_path: &str,
    ) -> Result<GeneratePreviewUrlResponse, Status> {
        let path = object_path.trim();
        if path.is_empty() {
            return Err(Status::invalid_argument("object_path wajib diisi"));
        }
        if path.len() > 1024 {
            return Err(Status::invalid_argument("object_path terlalu panjang"));
        }

        let runtime = self.runtime.clone();
        let path_owned = path.to_string();
        let (get_url, expires_at_unix) =
            self.preview_cache
                .get_or_load(path_owned.as_str(), || {
                    gcs_signed_get_url_for_stored_object_preview(
                        runtime.bucket.as_str(),
                        path_owned.as_str(),
                        runtime.preview_url_expires_secs,
                        &runtime.account,
                    )
                })?;

        Ok(GeneratePreviewUrlResponse {
            get_url,
            expires_at_unix,
        })
    }
}

fn validate_gcs_module(module: &str) -> Result<&str, Status> {
    let module = module.trim();
    let module = if module.is_empty() {
        MODULE_STOKSAHAM
    } else {
        module
    };
    if module.len() > 255 {
        return Err(Status::invalid_argument("module terlalu panjang"));
    }
    if module.contains('/') || module.contains("..") {
        return Err(Status::invalid_argument(
            "module harus satu segmen path tanpa slash",
        ));
    }
    Ok(module)
}

#[tonic::async_trait]
impl GcsRpc for GcsGrpcService {
    async fn get_storage_url(
        &self,
        request: Request<GetStorageUrlRequest>,
    ) -> Result<Response<GetStorageUrlResponse>, Status> {
        let claims = require_auth(&request)?;
        let req = request.into_inner();
        let (ext, _, _) = gcs_upload_extension(req.file_extension.as_str())?;
        let module = validate_gcs_module(req.module.as_str())?;

        // stockbit tidak punya cabang — pakai user_id sebagai segmen path.
        let cabang = if claims.user_id.trim().is_empty() {
            "default"
        } else {
            claims.user_id.as_str()
        };
        let object_path = gcs_new_object_path(module, cabang, ext);

        let out = gcs_signed_post_upload(
            self.runtime.bucket.as_str(),
            req.file_extension.as_str(),
            self.runtime.max_foto_size_bytes,
            self.runtime.max_video_size_bytes,
            self.runtime.signed_url_expires_secs,
            &self.runtime.account,
            object_path,
        )?;

        Ok(Response::new(GetStorageUrlResponse {
            post_url: out.post_url,
            object_path: out.object_path,
            content_type: out.content_type,
            expires_at_unix: out.expires_at_unix,
            max_file_size_bytes: out.max_file_size_bytes,
            post_form_fields: out.post_form_fields,
        }))
    }

    async fn gcs_delete_object(
        &self,
        request: Request<GcsDeleteObjectRequest>,
    ) -> Result<Response<GcsDeleteObjectResponse>, Status> {
        let _claims = require_auth(&request)?;
        let req = request.into_inner();
        let wire = gcs_object_wire_path(self.runtime.bucket.as_str(), req.path.as_str())
            .ok_or_else(|| Status::invalid_argument("path object tidak valid"))?;

        match gcs_delete_object(
            self.runtime.bucket.as_str(),
            wire.as_str(),
            &self.runtime.account,
            &self.oauth,
        )
        .await
        {
            Ok(()) => Ok(Response::new(GcsDeleteObjectResponse {
                success: true,
                message: "ok".into(),
            })),
            Err(e) => Ok(Response::new(GcsDeleteObjectResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    async fn generate_preview_url(
        &self,
        request: Request<GeneratePreviewUrlRequest>,
    ) -> Result<Response<GeneratePreviewUrlResponse>, Status> {
        let _claims = require_auth(&request)?;
        let req = request.into_inner();
        let out = self.generate_preview_url_for_object_path(req.object_path.as_str())?;
        Ok(Response::new(out))
    }
}
