fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("hari_libur_descriptor.bin"))
        .compile_protos(&["src/hari_libur.proto"], &["src"])?;
    Ok(())
}
