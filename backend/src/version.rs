//! Compile-time version stamping (see build.rs: `stamp_git_sha`).
//!
//! `FULL` is what /api/version reports for the backend — crate version +
//! building commit, e.g. "0.1.0+df78fe2". The sha comes either from the
//! `RED_EYE_GIT_SHA` build arg (Docker) or from `git` at build time.

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_SHA: &str = env!("RED_EYE_GIT_SHA");
pub const FULL: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("RED_EYE_GIT_SHA"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_version_combines_version_and_sha() {
        assert_eq!(FULL, format!("{}+{}", VERSION, GIT_SHA));
        // Stamped by build.rs — must never be empty.
        assert!(!GIT_SHA.is_empty());
    }
}
