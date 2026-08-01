# invezgood

gRPC server minimal dengan [Tonic](https://github.com/hyperium/tonic).

## Menjalankan

```bash
cargo run
# atau release
cargo build --release && ./target/release/invezgood
```

Default listen: `0.0.0.0:50054` (env: `HOST`, `GRPC_PORT`).

## Deploy (PM2)

```bash
./build.sh
```

## Uji RPC Ping

```bash
grpcurl -plaintext -d '{"message":"hello"}' localhost:50054 invezgood.Invezgood/Ping
```
