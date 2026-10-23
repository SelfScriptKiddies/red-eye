fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_descriptors = protox::compile(["proto/wire.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(file_descriptors)?;
    println!("cargo:rerun-if-changed=proto/wire.proto");
    stamp_git_sha();
    Ok(())
}

/// Bake the building commit into the binary (`RED_EYE_GIT_SHA` env at
/// compile time, see src/version.rs). Explicit env wins (Docker builds
/// pass the sha via build arg — no .git in the image context), then `git`,
/// then "unknown". Serves /api/version drift detection.
fn stamp_git_sha() {
    let sha = std::env::var("RED_EYE_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RED_EYE_GIT_SHA={}", sha);
}
