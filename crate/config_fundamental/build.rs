//! Kompilasi `config_fundamental.proto` + descriptor set untuk gRPC reflection.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    let proto = manifest_dir.join("src/config_fundamental.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let protoc_path =
        protoc_bin_vendored::protoc_bin_path().map_err(|e| format!("protoc: {e}"))?;
    unsafe {
        std::env::set_var("PROTOC", &protoc_path);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(out_dir.join("config_fundamental_descriptor.bin"))
        .compile_protos(&[proto], &[manifest_dir.join("src")])?;
    Ok(())
}
