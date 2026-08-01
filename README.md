# invezgood

gRPC server modular dengan [Tonic](https://github.com/hyperium/tonic).

## Struktur

```
app/src/main.rs          # entry point — daftarkan layanan gRPC di sini
crate/stock_list/        # modul daftar saham
```

Setiap fitur = crate terpisah di `crate/<nama>/` (proto + service + lib.rs).

## Menjalankan

```bash
cargo run -p invezgood
# atau release
cargo build --release && ./target/release/invezgood
```

Default listen: `0.0.0.0:50054` (env: `HOST`, `GRPC_PORT`).

## MCP Invezgo (Cursor)

Invezgo MCP memakai **HTTP+SSE dengan session** (`mcp-session-id`). Konfigurasi `url` langsung sering gagal dengan error **`No sessionId`** (HTTP 400).

MCP Invezgo lewat script `.cursor/mcp-invezgo.sh` (source `.env` → `mcp-remote` dengan `--transport http-only`).

```json
{
  "mcpServers": {
    "invezgo": {
      "command": "/home/baki1/invezgood_rust/.cursor/mcp-invezgo.sh"
    }
  }
}
```

Token: `INVEZGO_BEARER_TOKEN` di `.env`. Restart Cursor setelah edit.

## Deploy (PM2)

```bash
./build.sh
```

## Uji RPC

```bash
# health ping
grpcurl -plaintext -d '{"message":"hello"}' localhost:50054 invezgood.Invezgood/Ping

# sync dari Invezgo → Scylla
grpcurl -plaintext -d '{}' localhost:50054 stock_list.StockList/GetStockListFromInvezgo

# baca semua baris dari Scylla (token ring scan)
grpcurl -plaintext -d '{}' localhost:50054 stock_list.StockList/GetStockListFromScylla
```
