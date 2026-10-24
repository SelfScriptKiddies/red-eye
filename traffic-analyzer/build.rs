//! Compile proto/wire.proto (top-level repo `proto/`, symlinked into this crate) into Rust types

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_descriptors = protox::compile(["proto/wire.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(file_descriptors)?;
    println!("cargo:rerun-if-changed=proto/wire.proto");
    stamp_git_sha();
    stamp_version();
    Ok(())
}

/// Bake the building commit into the binary (`RED_EYE_GIT_SHA` env at
/// compile time, see src/version.rs). Explicit env wins (Docker builds
/// pass the sha via build arg — no .git in the image context), then `git`,
/// then "unknown". Serves /api/version drift detection and `--version`.
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

/// Bake the release version into the binary (`RED_EYE_VERSION` env at
/// compile time, see src/version.rs). Explicit env wins (Docker builds
/// pass the version via build arg), then the repo-root VERSION file —
/// the canonical version source (see scripts/bump-version.sh) —, then
/// "unknown". Serves /api/version drift detection and `--version`.
fn stamp_version() {
    let version = std::env::var("RED_EYE_VERSION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("../VERSION")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rerun-if-changed=../VERSION");
    println!("cargo:rustc-env=RED_EYE_VERSION={}", version);
}
