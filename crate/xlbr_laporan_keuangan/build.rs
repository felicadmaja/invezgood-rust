fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("xlbr_laporan_keuangan_descriptor.bin"))
        .compile_protos(&["src/xlbr_laporan_keuangan.proto"], &["src"])?;
    Ok(())
}
