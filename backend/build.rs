fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_descriptors = protox::compile(["proto/wire.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(file_descriptors)?;
    println!("cargo:rerun-if-changed=proto/wire.proto");
    Ok(())
}
