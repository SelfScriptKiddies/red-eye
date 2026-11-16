use std::time::Duration;

// Process configuration from the environment. Everything defaults to its
// most restrictive value: read-only mode, full control requiring BOTH a
// mode flip and an explicit allow switch, tight request limits.

pub const DEFAULT_BACKEND_URL: &str = "http://backend:8000";
pub const DEFAULT_BIND: &str = "0.0.0.0:8081";

/// Bounded defaults; not operator-tunable per request.
pub const MAX_BODY_BYTES: usize = 256 * 1024;
pub const MAX_JSON_DEPTH: usize = 12;
pub const MAX_SESSIONS: usize = 64;
pub const MAX_CONCURRENT_CALLS: usize = 8;
pub const SESSION_IDLE_SECS: u64 = 30 * 60;
/// Requests per 60s window per token, then per source address.
pub const RATE_LIMIT_PER_TOKEN: u32 = 120;
pub const RATE_LIMIT_PER_ADDR: u32 = 240;
pub const RATE_WINDOW_SECS: u64 = 60;
/// One-shot dry-run operations expire after this many seconds.
pub const DRYRUN_TTL_SECS: u64 = 300;
pub const DRYRUN_MAX_OPS: usize = 32;
/// Backend authz/token validation call budget.
pub const BACKEND_TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    ReadOnly,
    FullControl,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub backend_url: String,
    pub service_token: String,
    pub mode: Mode,
    pub bind: String,
}

impl Config {
    /// Missing service token aborts startup: a half-configured MCP
    /// deployment must not come up accepting unvalidatable tokens.
    pub fn from_env() -> Result<Config, String> {
        let backend_url = std::env::var("RED_EYE_BACKEND_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_BACKEND_URL.to_string());
        // An empty token is allowed at boot: the service starts fail-closed
        // (every request rejected) so the compose stack comes up before an
        // operator generates the credential in Settings.
        let service_token = std::env::var("MCP_SERVICE_TOKEN")
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let mode = match std::env::var("MCP_MODE").unwrap_or_default().trim() {
            "" | "read_only" => Mode::ReadOnly,
            "full_control" => Mode::FullControl,
            other => {
                return Err(format!(
                    "unknown MCP_MODE '{}', use read_only or full_control",
                    other
                ));
            }
        };
        let allow = std::env::var("MCP_ALLOW_FULL_CONTROL")
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if mode == Mode::FullControl && !allow {
            return Err(
                "MCP_MODE=full_control also requires MCP_ALLOW_FULL_CONTROL=true: \
                 full control is a deliberate redeployment, never a default"
                    .to_string(),
            );
        }
        let bind = std::env::var("MCP_BIND")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_BIND.to_string());
        Ok(Config {
            backend_url: backend_url.trim_end_matches('/').to_string(),
            service_token,
            mode,
            bind,
        })
    }
}

pub fn backend_timeout() -> Duration {
    Duration::from_secs(BACKEND_TIMEOUT_SECS)
}
