//! Kompilasi `emiten_trending_count.proto` + descriptor set (`emiten_trending_count_descriptor.bin`)
//! untuk gRPC reflection — didaftarkan di `app` / bin via `emiten_trending_count::FILE_DESCRIPTOR_SET`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    let proto = manifest_dir.join("src/emiten_trending_count.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let protoc_path =
        protoc_bin_vendored::protoc_bin_path().map_err(|e| format!("protoc: {e}"))?;
    unsafe {
        std::env::set_var("PROTOC", &protoc_path);
    }

    let descriptor_path = out_dir.join("emiten_trending_count_descriptor.bin");
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&[proto], &[manifest_dir.join("src")])?;
    Ok(())
}
