//! Compile-time version stamping (see build.rs: `stamp_version` /
//! `stamp_git_sha`).
//!
//! `--version` prints `FULL` (release version + building commit, e.g.
//! "0.2.0+df78fe2"); the same string rides in the agent heartbeat so the
//! backend's /api/version can surface version drift across deployed agents
//! (releases ship from GitHub — see .github/workflows/release.yml).

pub const VERSION: &str = env!("RED_EYE_VERSION");
pub const GIT_SHA: &str = env!("RED_EYE_GIT_SHA");
pub const FULL: &str = concat!(env!("RED_EYE_VERSION"), "+", env!("RED_EYE_GIT_SHA"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_version_combines_version_and_sha() {
        assert_eq!(FULL, format!("{}+{}", VERSION, GIT_SHA));
        assert!(!GIT_SHA.is_empty());
        assert!(!VERSION.is_empty());
    }
}
