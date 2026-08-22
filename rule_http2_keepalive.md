# Aturan timing keepalive gRPC (Tonic / HTTP/2)

Server gRPC (`app/src/main.rs`) memakai **dua lapisan** keepalive supaya koneksi mati (TCP RST, timeout, client hilang) terdeteksi cepat dan **task stream Tokio segera dibatalkan**.

Implementasi: `Server::tcp_keepalive`, `Server::http2_keepalive_interval`, `Server::http2_keepalive_timeout` (Tonic 0.12 → hyper/h2).

---

## Default timing

| Parameter | Env | Default | Arti |
|-----------|-----|---------|------|
| TCP idle probe | `GRPC_TCP_KEEPALIVE_SECS` | **30 detik** | Setelah idle N detik, OS mulai kirim TCP keepalive probe ke socket |
| HTTP/2 PING interval | `GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS` | **30 detik** | Server kirim frame HTTP/2 PING ke client setiap N detik |
| HTTP/2 PING timeout | `GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS` | **10 detik** | Jika PING tidak di-ACK dalam N detik, koneksi ditutup |

**Worst-case deteksi koneksi mati (HTTP/2):** `interval + timeout` = **40 detik** (30 + 10).

**Nonaktifkan:** set env ke `0` → Tonic menerima `None` (keepalive layer itu dimatikan).

---

## Alur HTTP/2 keepalive

1. Koneksi idle (tidak ada frame data) selama **interval** → hyper/h2 kirim **PING**.
2. Client wajib balas **PING ACK** dalam **timeout**.
3. Timeout habis → hyper menutup koneksi → semua stream gRPC pada koneksi itu **cancel** → handler/stream task Tokio drop.

PING berjalan **per koneksi**, bukan per RPC. Satu konegsi mati = semua stream di koneksi itu ikut putus.

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

- Konfigurasi: `app/src/main.rs` → `apply_grpc_keepalive()`
- Contoh env: `.env-example` (blok `GRPC_*_KEEPALIVE_*`)

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

