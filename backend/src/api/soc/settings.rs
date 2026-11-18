use sqlx::PgPool;
use std::collections::HashMap;

// SOC settings live in app_settings (plain string values) and are re-read
// on every worker-facing request, so runtime changes apply without
// restarting the backend or the worker. Defaults mirror the seeded rows
// in database/migrations/add_soc_and_mcp.sql.

pub const SOC_ENABLED: &str = "soc_enabled";
pub const SOC_AUTO_ENABLED: &str = "soc_auto_enabled";
pub const SOC_BASE_URL: &str = "soc_base_url";
pub const SOC_API_KEY: &str = "soc_api_key";
pub const SOC_MODEL: &str = "soc_model";
pub const SOC_ALERT_THRESHOLD: &str = "soc_alert_threshold";
pub const SOC_AUTO_MIN_JEV_SEVERITY: &str = "soc_auto_min_jev_severity";
pub const SOC_MAX_CONTEXT_TOKENS: &str = "soc_max_context_tokens";
pub const SOC_MAX_OUTPUT_TOKENS: &str = "soc_max_output_tokens";
pub const SOC_MAX_TOOL_CALLS: &str = "soc_max_tool_calls";
pub const SOC_MAX_TOOL_RESULT_BYTES: &str = "soc_max_tool_result_bytes";
pub const SOC_JOB_TIMEOUT_SECS: &str = "soc_job_timeout_secs";
pub const SOC_MAX_ATTEMPTS: &str = "soc_max_attempts";
pub const SOC_CONCURRENCY: &str = "soc_concurrency";
pub const SOC_DAILY_JOB_LIMIT: &str = "soc_daily_job_limit";

// Write-only secrets: never returned by any GET. soc_worker_token and
// mcp_service_token are backend-generated (SHA-256 stored) via dedicated
// operator endpoints; direct PUTs are rejected.
pub const SOC_WORKER_TOKEN: &str = "soc_worker_token";
pub const MCP_SERVICE_TOKEN: &str = "mcp_service_token";
// Backend-side scope of the MCP service credential: read_only or
// full_control. Full-control backend mutation endpoints require
// full_control here INDEPENDENTLY of the MCP-layer token capability.
pub const MCP_SERVICE_CAPABILITY: &str = "mcp_service_capability";

// Backend-managed enablement generation: incremented on every
// false->true transition of soc_enabled. Jobs record it; late results
// from an older generation are rejected.
pub const SOC_SETTINGS_GENERATION: &str = "soc_settings_generation";

// Backend-owned triage prompt format version. Recorded on every job and
// result; a bump invalidates in-flight work identities.
pub const SOC_PROMPT_VERSION: i32 = 1;

const KEYS: [&str; 17] = [
    SOC_ENABLED,
    SOC_AUTO_ENABLED,
    SOC_BASE_URL,
    SOC_API_KEY,
    SOC_MODEL,
    SOC_ALERT_THRESHOLD,
    SOC_AUTO_MIN_JEV_SEVERITY,
    SOC_MAX_CONTEXT_TOKENS,
    SOC_MAX_OUTPUT_TOKENS,
    SOC_MAX_TOOL_CALLS,
    SOC_MAX_TOOL_RESULT_BYTES,
    SOC_JOB_TIMEOUT_SECS,
    SOC_MAX_ATTEMPTS,
    SOC_CONCURRENCY,
    SOC_DAILY_JOB_LIMIT,
    MCP_SERVICE_CAPABILITY,
    SOC_SETTINGS_GENERATION,
];

const DEFAULT_ENABLED: bool = false;
const DEFAULT_AUTO_ENABLED: bool = false;
const DEFAULT_BASE_URL: &str = "http://llm:8080/v1";
const DEFAULT_MODEL: &str = "qwen3.8-flash-next-iq3";
const DEFAULT_ALERT_THRESHOLD: f64 = 0.7;
const DEFAULT_AUTO_MIN_JEV_SEVERITY: i64 = 2;
const DEFAULT_MAX_CONTEXT_TOKENS: i64 = 16384;
const DEFAULT_MAX_OUTPUT_TOKENS: i64 = 1024;
const DEFAULT_MAX_TOOL_CALLS: i64 = 8;
const DEFAULT_MAX_TOOL_RESULT_BYTES: i64 = 16384;
const DEFAULT_JOB_TIMEOUT_SECS: i64 = 120;
const DEFAULT_MAX_ATTEMPTS: i64 = 3;
const DEFAULT_CONCURRENCY: i64 = 1;
const DEFAULT_DAILY_JOB_LIMIT: i64 = 500;

const QUEUE_CAP: i64 = 1000;

#[derive(Debug, Clone)]
pub struct SocSettings {
    pub enabled: bool,
    pub auto_enabled: bool,
    pub base_url: String,
    pub api_key: String,
    pub api_key_configured: bool,
    pub model: String,
    pub alert_threshold: f64,
    pub auto_min_jev_severity: i64,
    pub max_context_tokens: i64,
    pub max_output_tokens: i64,
    pub max_tool_calls: i64,
    pub max_tool_result_bytes: i64,
    pub job_timeout_secs: i64,
    pub max_attempts: i64,
    pub concurrency: i64,
    pub daily_job_limit: i64,
    pub settings_generation: i64,
}

impl SocSettings {
    /// Read every soc_* key in one query. Garbage values fall back to the
    /// defaults instead of error-looping: the PUT path rejects invalid
    /// writes, so a fallback here only covers rows edited outside the API.
    pub async fn load(pool: &PgPool) -> SocSettings {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key, value FROM app_settings WHERE key = ANY($1)")
                .bind(KEYS.map(|k| k.to_string()).to_vec())
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        let map: HashMap<String, String> = rows.into_iter().collect();

        let base_url = map
            .get(SOC_BASE_URL)
            .map(|v| v.trim().to_string())
            .filter(|v| {
                (v.starts_with("http://") || v.starts_with("https://"))
                    && v.len() <= 512
                    && v.chars().all(|c| c.is_ascii_graphic())
            })
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let model = map
            .get(SOC_MODEL)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty() && v.len() <= 128)
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());

        let api_key = map.get(SOC_API_KEY).cloned().unwrap_or_default();
        SocSettings {
            enabled: map
                .get(SOC_ENABLED)
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(DEFAULT_ENABLED),
            auto_enabled: map
                .get(SOC_AUTO_ENABLED)
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(DEFAULT_AUTO_ENABLED),
            base_url,
            api_key_configured: !api_key.trim().is_empty(),
            api_key,
            model,
            alert_threshold: map
                .get(SOC_ALERT_THRESHOLD)
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| (0.0..=1.0).contains(v))
                .unwrap_or(DEFAULT_ALERT_THRESHOLD),
            auto_min_jev_severity: map
                .get(SOC_AUTO_MIN_JEV_SEVERITY)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (0..=3).contains(v))
                .unwrap_or(DEFAULT_AUTO_MIN_JEV_SEVERITY),
            max_context_tokens: map
                .get(SOC_MAX_CONTEXT_TOKENS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (256..=1_000_000).contains(v))
                .unwrap_or(DEFAULT_MAX_CONTEXT_TOKENS),
            max_output_tokens: map
                .get(SOC_MAX_OUTPUT_TOKENS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (64..=32_000).contains(v))
                .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS),
            max_tool_calls: map
                .get(SOC_MAX_TOOL_CALLS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=64).contains(v))
                .unwrap_or(DEFAULT_MAX_TOOL_CALLS),
            max_tool_result_bytes: map
                .get(SOC_MAX_TOOL_RESULT_BYTES)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (256..=4_000_000).contains(v))
                .unwrap_or(DEFAULT_MAX_TOOL_RESULT_BYTES),
            job_timeout_secs: map
                .get(SOC_JOB_TIMEOUT_SECS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (10..=3600).contains(v))
                .unwrap_or(DEFAULT_JOB_TIMEOUT_SECS),
            max_attempts: map
                .get(SOC_MAX_ATTEMPTS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=10).contains(v))
                .unwrap_or(DEFAULT_MAX_ATTEMPTS),
            concurrency: map
                .get(SOC_CONCURRENCY)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=32).contains(v))
                .unwrap_or(DEFAULT_CONCURRENCY),
            daily_job_limit: map
                .get(SOC_DAILY_JOB_LIMIT)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=100_000).contains(v))
                .unwrap_or(DEFAULT_DAILY_JOB_LIMIT),
            settings_generation: map
                .get(SOC_SETTINGS_GENERATION)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(0),
        }
    }
}

/// PUT-side validation. Returns Err(message) for a rejected value; the
/// caller keeps the previous setting untouched. `soc_api_key` accepts any
/// string (an empty string clears the key) - secrecy, not validation.
/// The generated credentials (soc_worker_token, mcp_service_token) are
/// never written through this path.
pub async fn validate_setting(key: &str, value: &str, _pool: &PgPool) -> Result<(), String> {
    let v = value.trim();
    match key {
        SOC_ENABLED => {
            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
                Ok(())
            } else {
                Err("soc_enabled must be true or false".to_string())
            }
        }
        SOC_AUTO_ENABLED => {
            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
                Ok(())
            } else {
                Err("soc_auto_enabled must be true or false".to_string())
            }
        }
        SOC_BASE_URL => {
            let ok = (v.starts_with("http://") || v.starts_with("https://"))
                && !v.is_empty()
                && v.len() <= 512
                && v.chars().all(|c| c.is_ascii_graphic());
            if ok {
                Ok(())
            } else {
                Err(
                    "soc_base_url must be an http(s) URL of at most 512 printable ASCII characters"
                        .to_string(),
                )
            }
        }
        SOC_API_KEY => {
            if value.len() <= 4096 {
                Ok(())
            } else {
                Err("soc_api_key must be at most 4096 characters".to_string())
            }
        }
        SOC_MODEL => {
            let ok = !v.is_empty() && v.len() <= 128 && v.chars().all(|c| c.is_ascii_graphic());
            if ok {
                Ok(())
            } else {
                Err("soc_model must be 1-128 printable ASCII characters without spaces".to_string())
            }
        }
        SOC_ALERT_THRESHOLD => parse_range(
            v,
            0.0..=1.0,
            "soc_alert_threshold must be a number from 0 to 1",
        ),
        SOC_AUTO_MIN_JEV_SEVERITY => parse_int_range(
            v,
            0..=3,
            "soc_auto_min_jev_severity must be an integer from 0 to 3",
        ),
        SOC_MAX_CONTEXT_TOKENS => parse_int_range(
            v,
            256..=1_000_000,
            "soc_max_context_tokens must be an integer from 256 to 1000000",
        ),
        SOC_MAX_OUTPUT_TOKENS => parse_int_range(
            v,
            64..=32_000,
            "soc_max_output_tokens must be an integer from 64 to 32000",
        ),
        SOC_MAX_TOOL_CALLS => parse_int_range(
            v,
            1..=64,
            "soc_max_tool_calls must be an integer from 1 to 64",
        ),
        SOC_MAX_TOOL_RESULT_BYTES => parse_int_range(
            v,
            256..=4_000_000,
            "soc_max_tool_result_bytes must be an integer from 256 to 4000000",
        ),
        SOC_JOB_TIMEOUT_SECS => parse_int_range(
            v,
            10..=3600,
            "soc_job_timeout_secs must be an integer from 10 to 3600",
        ),
        SOC_MAX_ATTEMPTS => parse_int_range(
            v,
            1..=10,
            "soc_max_attempts must be an integer from 1 to 10",
        ),
        SOC_CONCURRENCY => {
            parse_int_range(v, 1..=32, "soc_concurrency must be an integer from 1 to 32")
        }
        SOC_DAILY_JOB_LIMIT => parse_int_range(
            v,
            1..=100_000,
            "soc_daily_job_limit must be an integer from 1 to 100000",
        ),
        MCP_SERVICE_CAPABILITY => {
            if v == "read_only" || v == "full_control" {
                Ok(())
            } else {
                Err("mcp_service_capability must be read_only or full_control".to_string())
            }
        }
        SOC_WORKER_TOKEN | MCP_SERVICE_TOKEN | SOC_SETTINGS_GENERATION => {
            Err(format!("{} is managed by the backend and read-only", key))
        }
        _ => Ok(()),
    }
}

fn parse_int_range(v: &str, range: std::ops::RangeInclusive<i64>, msg: &str) -> Result<(), String> {
    v.parse::<i64>()
        .ok()
        .filter(|n| range.contains(n))
        .map(|_| ())
        .ok_or_else(|| msg.to_string())
}

fn parse_range(v: &str, range: std::ops::RangeInclusive<f64>, msg: &str) -> Result<(), String> {
    v.parse::<f64>()
        .ok()
        .filter(|n| n.is_finite() && range.contains(n))
        .map(|_| ())
        .ok_or_else(|| msg.to_string())
}

/// GET-side masking for every write-only SOC/MCP secret: removes the key
/// and inserts a bare configured flag. No masked value, no echo.
pub fn mask_soc_secrets_in_map(map: &mut HashMap<String, String>) {
    for (key, flag) in [
        (SOC_API_KEY, "soc_api_key_configured"),
        (SOC_WORKER_TOKEN, "soc_worker_token_configured"),
        (MCP_SERVICE_TOKEN, "mcp_service_token_configured"),
    ] {
        let configured = map.get(key).map(|v| !v.trim().is_empty()).unwrap_or(false);
        map.remove(key);
        map.insert(flag.to_string(), configured.to_string());
    }
}

pub fn queue_cap() -> i64 {
    QUEUE_CAP
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soc_bounds_reject_out_of_range() {
        assert!(parse_range("0.5", 0.0..=1.0, "x").is_ok());
        assert!(parse_range("1.01", 0.0..=1.0, "x").is_err());
        assert!(parse_int_range("0", 0..=3, "x").is_ok());
        assert!(parse_int_range("4", 0..=3, "x").is_err());
        assert!(parse_int_range("1001", 1..=1000, "x").is_err());
    }

    #[tokio::test]
    async fn base_url_requires_http_scheme() {
        // validate_setting for this key returns before any query; the lazy
        // pool handle exists only to satisfy the signature.
        let pool = PgPool::connect_lazy("postgres://invalid:0/db").unwrap();
        assert!(
            validate_setting(SOC_BASE_URL, "http://llm:8080/v1", &pool)
                .await
                .is_ok()
        );
        assert!(
            validate_setting(SOC_BASE_URL, "ftp://x", &pool)
                .await
                .is_err()
        );
        assert!(
            validate_setting(SOC_BASE_URL, "http://bad\u{0}url", &pool)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn generated_secrets_reject_direct_put() {
        let pool = PgPool::connect_lazy("postgres://invalid:0/db").unwrap();
        assert!(
            validate_setting(SOC_WORKER_TOKEN, "x", &pool)
                .await
                .is_err()
        );
        assert!(
            validate_setting(MCP_SERVICE_TOKEN, "x", &pool)
                .await
                .is_err()
        );
        assert!(
            validate_setting(SOC_SETTINGS_GENERATION, "5", &pool)
                .await
                .is_err()
        );
    }

    #[test]
    fn masking_replaces_every_secret_with_a_flag() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(SOC_API_KEY.to_string(), "sk-secret".to_string());
        map.insert(SOC_WORKER_TOKEN.to_string(), "  ".to_string());
        mask_soc_secrets_in_map(&mut map);
        assert!(!map.contains_key(SOC_API_KEY));
        assert!(!map.contains_key(SOC_WORKER_TOKEN));
        assert!(!map.contains_key(MCP_SERVICE_TOKEN));
        assert_eq!(
            map.get("soc_api_key_configured").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(
            map.get("soc_worker_token_configured").map(|s| s.as_str()),
            Some("false")
        );
        assert_eq!(
            map.get("mcp_service_token_configured").map(|s| s.as_str()),
            Some("false")
        );
    }

    #[test]
    fn settings_defaults_cover_the_spec() {
        // Spec-named settings must exist as constants so a rename cannot
        // silently break the worker contract.
        assert_eq!(SOC_MAX_CONTEXT_TOKENS, "soc_max_context_tokens");
        assert_eq!(SOC_MAX_TOOL_CALLS, "soc_max_tool_calls");
        assert_eq!(SOC_DAILY_JOB_LIMIT, "soc_daily_job_limit");
    }
}
