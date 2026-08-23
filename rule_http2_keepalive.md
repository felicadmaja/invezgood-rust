# Aturan timing keepalive gRPC (Tonic / HTTP/2)

Server gRPC memakai **`grpc_server::apply_grpc_transport`** (`crate/grpc_server`) + patch Tonic (`third_party/tonic`) untuk keepalive, idle GOAWAY, dan max connection age.

Implementasi: `Server::tcp_keepalive`, `http2_keepalive_*`, `max_connection_idle`, `max_connection_age`, `max_connection_age_grace`.

---

## Default timing

| Parameter | Env | Default | Arti |
|-----------|-----|---------|------|
| TCP idle probe | `GRPC_TCP_KEEPALIVE_SECS` | **30 detik** | Setelah idle N detik, OS mulai kirim TCP keepalive probe ke socket |
| HTTP/2 PING interval | `GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS` | **30 detik** | Server kirim frame HTTP/2 PING ke client setiap N detik |
| HTTP/2 PING timeout | `GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS` | **10 detik** | Jika PING tidak di-ACK dalam N detik, koneksi ditutup |
| Max connection **idle** | `GRPC_HTTP2_MAX_CONNECTION_IDLE_SECS` | **300 detik (5 menit)** | Tanpa RPC → graceful **GOAWAY**; timer reset tiap request |
| Max connection **age** | `GRPC_HTTP2_MAX_CONNECTION_AGE_SECS` | **0 (nonaktif)** | Umur maks koneksi → GOAWAY → client renegotiasi channel baru |
| Age **grace** | `GRPC_HTTP2_MAX_CONNECTION_AGE_GRACE_SECS` | **30 detik** | Setelah age GOAWAY, tunggu drain lalu force-close bila perlu |
| PING tanpa RPC aktif | `GRPC_HTTP2_PERMIT_KEEPALIVE_WITHOUT_CALLS` | **true** | Hyper server: PING tetap jalan saat idle bila interval aktif |

**Nonaktifkan:** set env ke `0` (kecuali `PERMIT_*` → `false`).

---

## Graceful GOAWAY (Swift gRPC client)

1. **Idle timeout** — koneksi tidak menerima RPC selama `MAX_CONNECTION_IDLE` → `graceful_shutdown()` → frame GOAWAY → Swift buat subchannel baru pada request berikutnya.
2. **Max age** — koneksi hidup ≥ `MAX_CONNECTION_AGE` → GOAWAY → grace period → force-close bila client tidak selesai drain.
3. **PING gagal** — client mati/ suspended → koneksi putus (bukan GOAWAY graceful, tapi stream task server tetap dibersihkan).

Timer idle **reset** pada setiap inbound RPC (unary maupun stream).

---

## Aturan tuning

### 1. Timeout < interval (disarankan)

```
GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS < GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS
```

Contoh default (30 / 10) sudah benar. Jangan balik (timeout ≥ interval) — deteksi jadi lambat dan perilaku h2 kurang prediktif.

### 2. Client harus selaras (bila pakai keepalive di client)

gRPC client (mis. `tonic::transport::Channel`) punya `keep_alive_timeout` sendiri. **Server timeout tidak otomatis sinkron dengan client.**

- Client timeout **lebih kecil** dari server → client bisa putus duluan.
- Untuk long-lived stream, pastikan client **tidak** mematikan keepalive atau set interval/timeout yang kompatibel.

### 3. Streaming RPC idle lama

RPC server-streaming / bidirectional yang **tidak mengirim data lama** tetap aman selama koneksi hidup — PING/ACK tidak butuh traffic aplikasi.

Jika stream idle > `interval + timeout` **dan** client tidak merespons PING → koneksi dianggap mati (by design).

Perpanjang interval/timeout via env bila stream memang boleh idle lebih lama:

```bash
GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS=60
GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS=20
```

### 4. TCP vs HTTP/2 — kapan mana yang menang

| Lapisan | Deteksi | Catatan |
|---------|---------|---------|
| **HTTP/2 PING** | Client hidup di level protokol gRPC | Lebih cepat & eksplisit untuk stack Tonic/hyper |
| **TCP keepalive** | Socket mati di OS (RST, middlebox, NAT timeout) | Tonic hanya set **idle time** probe; interval/retry probe mengikuti **sysctl OS** (`tcp_keepalive_intvl`, `tcp_keepalive_probes`) |

Keduanya aktif by default. Matikan salah satu dengan env `=0` hanya jika ada alasan operasional jelas.

### 5. TLS

Keepalive berlaku **setelah** TLS handshake pada koneksi yang sama (`USE_TLS=true`). Timing HTTP/2 tidak berubah.

---

## Verifikasi saat startup

Log server:

```
gRPC keepalive: tcp=30s, http2_ping=30s, http2_timeout=10s
```

Nilai `0` = layer nonaktif.

---

## Referensi kode

- Konfigurasi env: `crate/grpc_server/src/lib.rs` → `apply_grpc_transport()`
- Entry: `app/src/main.rs`
- Patch GOAWAY/idle: `third_party/tonic/src/transport/server/mod.rs`
- Contoh env: `.env-example`

---

## Pola mpsc + ReceiverStream (handler streaming)

Keepalive TCP/HTTP/2 menutup koneksi transport; handler Rust harus **segera hentikan task** saat client disconnect.

### Mekanisme

```
Client disconnect
  → ReceiverStream drop
  → rx (mpsc receiver) drop
  → tx.send() → Err(SendError)
  → tx.closed() selesai
  → break loop / return dari tokio::spawn
```

### Aturan wajib

1. **Channel kecil** — buffer 8–32 cukup; buffer penuh + client lambat juga memicu `send` gagal.
2. **`send_or_break`** — setiap `tx.send()` cek error; `false` → `break`/`return` segera.
3. **Long-poll / sleep** — jangan `sleep` polos; pakai `sleep_unless_disconnected(&tx, dur)` atau `tokio::select! { tx.closed() ... }`.
4. **Fetch/API lama** — race dengan `tx.closed()` via `select!` agar task tidak lanjut scrape/API setelah client pergi.
5. **Cleanup** — setelah break, unregister subscriber / lepas resource di akhir task (defer pattern).

### Helper (`crate/grpc_stream`)

```rust
use grpc_stream::{send_or_break, sleep_unless_disconnected};

// Kirim chunk stream
if !send_or_break(&tx, Ok(item)).await {
    break; // client disconnect
}

// Long-poll loop
loop {
    // ... siapkan payload ...
    if !send_or_break(&tx, Ok(payload)).await {
        break;
    }
    if !sleep_unless_disconnected(&tx, Duration::from_secs(2)).await {
        break;
    }
}

// Fetch berat — abort bila client pergi
let data = tokio::select! {
    biased;
    () = tx.closed() => return,
    result = expensive_fetch() => result,
};
```

### Crate yang memakai pola ini

- `user` — `IsStockbitReady`, `GetPriceSpikeFromYahooFinance`
- `stock_list` — streaming Stockbit / repeated code / all keystats
- `bandarmology`, `hari_libur`, `top_gainer_loser`

