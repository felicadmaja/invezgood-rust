fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/stock_list.proto")?;
    Ok(())
}
