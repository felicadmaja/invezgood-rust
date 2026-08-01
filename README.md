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

Konfigurasi di `.cursor/mcp.json` (project) dan `~/.cursor/mcp.json` (global).

Token: set `INVEZGO_BEARER_TOKEN` di `.env`, lalu export ke environment sebelum buka Cursor:

```bash
set -a && source .env && set +a
cursor .
```

Reload window setelah itu (**Cursor Settings → MCP** → server `invezgo` harus Connected).

## Deploy (PM2)

```bash
./build.sh
```

## Uji RPC

```bash
# health ping
grpcurl -plaintext -d '{"message":"hello"}' localhost:50054 invezgood.Invezgood/Ping

# stock list
grpcurl -plaintext -d '{"limit":5}' localhost:50054 stock_list.StockList/List
grpcurl -plaintext -d '{}' localhost:50054 stock_list.StockList/GetStockListFromInvezgo
```
