fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto = manifest_dir.join("src/user.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let protoc_path =
        protoc_bin_vendored::protoc_bin_path().map_err(|e| format!("protoc: {e}"))?;
    unsafe {
        std::env::set_var("PROTOC", &protoc_path);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &[manifest_dir.join("src")])?;
    Ok(())
}
