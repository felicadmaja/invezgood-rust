//! Konfigurasi transport gRPC server (keepalive, idle GOAWAY, max connection age).

use std::time::Duration;

use tonic::transport::Server;

/// Idle sebelum GOAWAY (tanpa RPC aktif); timer reset tiap request.
const DEFAULT_MAX_CONNECTION_IDLE_SECS: u64 = 300;
/// Grace setelah max connection age GOAWAY sebelum force-close.
const DEFAULT_MAX_CONNECTION_AGE_GRACE_SECS: u64 = 30;
const DEFAULT_TCP_KEEPALIVE_SECS: u64 = 30;
const DEFAULT_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 30;
const DEFAULT_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 10;

fn duration_secs_from_env(key: &str, default_secs: u64) -> Option<Duration> {
    match std::env::var(key) {
        Ok(raw) => {
            let secs: u64 = raw.parse().unwrap_or(default_secs);
            if secs == 0 {
                None
            } else {
                Some(Duration::from_secs(secs))
            }
        }
        Err(_) => Some(Duration::from_secs(default_secs)),
    }
}

/// `0` = nonaktif. Env tidak di-set → `None` (tidak dipasang ke builder).
fn optional_duration_secs_from_env(key: &str) -> Option<Duration> {
    match std::env::var(key) {
        Ok(raw) => {
            let secs: u64 = raw.parse().unwrap_or(0);
            if secs == 0 {
                None
            } else {
                Some(Duration::from_secs(secs))
            }
        }
        Err(_) => None,
    }
}

fn bool_from_env(key: &str, default: bool) -> bool {
    std::env::var(key)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(default)
}

/// Terapkan TCP/HTTP2 keepalive + idle/age GOAWAY ke [`Server`] builder.
///
/// `permit_keepalive_without_calls`: hyper server sudah set `keep_alive_while_idle=true`
/// saat `http2_keepalive_interval` aktif — PING tetap jalan meski tidak ada RPC/stream.
pub fn apply_grpc_transport(mut builder: Server) -> Server {
    let tcp_keepalive =
        duration_secs_from_env("GRPC_TCP_KEEPALIVE_SECS", DEFAULT_TCP_KEEPALIVE_SECS);
    let http2_interval = duration_secs_from_env(
        "GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS",
        DEFAULT_HTTP2_KEEPALIVE_INTERVAL_SECS,
    );
    let http2_timeout = duration_secs_from_env(
        "GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS",
        DEFAULT_HTTP2_KEEPALIVE_TIMEOUT_SECS,
    );
    let max_connection_idle = duration_secs_from_env(
        "GRPC_HTTP2_MAX_CONNECTION_IDLE_SECS",
        DEFAULT_MAX_CONNECTION_IDLE_SECS,
    );
    let max_connection_age = optional_duration_secs_from_env("GRPC_HTTP2_MAX_CONNECTION_AGE_SECS");
    let max_connection_age_grace_env =
        optional_duration_secs_from_env("GRPC_HTTP2_MAX_CONNECTION_AGE_GRACE_SECS");
    let permit_keepalive_without_calls =
        bool_from_env("GRPC_HTTP2_PERMIT_KEEPALIVE_WITHOUT_CALLS", true);

    println!(
        "gRPC transport: tcp={}s ping={}s ping_timeout={}s idle_goaway={}s age={}s age_grace={}s permit_ping_without_calls={}",
        tcp_keepalive.map(|d| d.as_secs()).unwrap_or(0),
        http2_interval.map(|d| d.as_secs()).unwrap_or(0),
        http2_timeout.map(|d| d.as_secs()).unwrap_or(0),
        max_connection_idle.map(|d| d.as_secs()).unwrap_or(0),
        max_connection_age.map(|d| d.as_secs()).unwrap_or(0),
        max_connection_age_grace_env
            .unwrap_or(Duration::from_secs(DEFAULT_MAX_CONNECTION_AGE_GRACE_SECS))
            .as_secs(),
        permit_keepalive_without_calls,
    );

    if !permit_keepalive_without_calls {
        eprintln!(
            "\x1b[33mGRPC_HTTP2_PERMIT_KEEPALIVE_WITHOUT_CALLS=false — hyper server tetap \
             mengirim HTTP/2 PING saat idle bila interval keepalive aktif (perilaku bawaan)\x1b[0m"
        );
    }

    builder = builder
        .tcp_keepalive(tcp_keepalive)
        .http2_keepalive_interval(http2_interval)
        .http2_keepalive_timeout(http2_timeout);

    if let Some(idle) = max_connection_idle {
        builder = builder.max_connection_idle(idle);
    }
    if let Some(age) = max_connection_age {
        builder = builder.max_connection_age(age);
        let grace = max_connection_age_grace_env
            .unwrap_or(Duration::from_secs(DEFAULT_MAX_CONNECTION_AGE_GRACE_SECS));
        builder = builder.max_connection_age_grace(grace);
    }

    builder
}
