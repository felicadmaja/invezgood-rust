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
```
