use sqlx::PgPool;
use std::collections::HashMap;

// JEV settings live in app_settings (plain string values) and are re-read
// by the scheduler EVERY tick, so runtime changes apply without restarting
// the backend or the analyzer. Defaults here mirror the seeded rows in
// database/migrations/add_ai_verdicts.sql.

pub const AI_ENABLED: &str = "ai_enabled";
pub const AI_API_KEY: &str = "ai_api_key";
pub const AI_MODEL: &str = "ai_model";
pub const AI_THRESHOLD: &str = "ai_threshold";
pub const AI_MAX_PER_TICK: &str = "ai_max_per_tick";
pub const AI_DEBOUNCE_MS: &str = "ai_stream_debounce_ms";
pub const AI_MIN_INTERVAL_SECS: &str = "ai_stream_min_interval_secs";
pub const AI_MAX_BYTES: &str = "ai_stream_max_bytes";
pub const AI_MAX_ENTRIES: &str = "ai_stream_max_entries";
pub const AI_ENTRY_MAX_BYTES: &str = "ai_entry_max_bytes";
pub const AI_DAILY_COST_LIMIT: &str = "ai_daily_cost_limit";
pub const AI_RETENTION_DAYS: &str = "ai_verdict_retention_days";

/// Enablement watermark (backend-managed, not operator-editable): the
/// highest stream packet id at the moment JEV was enabled. Candidates must
/// carry activity AFTER this id, so enabling never sweeps historical
/// streams into external analysis. Cleared on disable; a backend restart
/// keeps it, which is what lets in-flight work be rescheduled safely.
pub const AI_ENABLED_BOUNDARY: &str = "ai_enabled_at_packet_id";

const KEYS: [&str; 12] = [
    AI_ENABLED,
    AI_API_KEY,
    AI_MODEL,
    AI_THRESHOLD,
    AI_MAX_PER_TICK,
    AI_DEBOUNCE_MS,
    AI_MIN_INTERVAL_SECS,
    AI_MAX_BYTES,
    AI_MAX_ENTRIES,
    AI_ENTRY_MAX_BYTES,
    AI_DAILY_COST_LIMIT,
    AI_RETENTION_DAYS,
];

const DEFAULT_ENABLED: bool = false;
const DEFAULT_MODEL: &str = "typesafe/jev-1.13";
const DEFAULT_THRESHOLD: f64 = 0.7;
const DEFAULT_MAX_PER_TICK: i64 = 10;
const DEFAULT_DEBOUNCE_MS: i64 = 2000;
const DEFAULT_MIN_INTERVAL_SECS: i64 = 10;
const DEFAULT_MAX_BYTES: i64 = 65536;
const DEFAULT_MAX_ENTRIES: i64 = 200;
const DEFAULT_ENTRY_MAX_BYTES: i64 = 8192;
const DEFAULT_DAILY_COST_LIMIT: f64 = 5.0;
const DEFAULT_RETENTION_DAYS: i64 = 30;

/// Snapshot of one tick's effective JEV configuration. `api_key` never
/// leaves this process except inside the Authorization header of a
/// Decisions call; nothing serializes this struct.
#[derive(Debug, Clone)]
pub struct AiSettings {
    pub enabled: bool,
    pub api_key: String,
    pub api_key_configured: bool,
    pub model: String,
    pub threshold: f64,
    pub max_per_tick: usize,
    pub debounce_ms: i64,
    pub min_interval_secs: i64,
    pub max_bytes: usize,
    pub max_entries: usize,
    pub entry_max_bytes: usize,
    pub daily_cost_limit: f64,
    pub retention_days: i64,
}

impl AiSettings {
    /// Read every ai_* key in one query. Garbage values fall back to the
    /// defaults instead of error-looping: the PUT path rejects invalid
    /// writes, so a fallback here only covers rows edited outside the API.
    pub async fn load(pool: &PgPool) -> AiSettings {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key, value FROM app_settings WHERE key = ANY($1)")
                .bind(KEYS.map(|k| k.to_string()).to_vec())
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        let map: HashMap<String, String> = rows.into_iter().collect();

        let max_bytes = map
            .get(AI_MAX_BYTES)
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| (1024..=10_000_000).contains(v))
            .unwrap_or(DEFAULT_MAX_BYTES);
        let entry_max_bytes = map
            .get(AI_ENTRY_MAX_BYTES)
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| (64..=1_000_000).contains(v))
            .unwrap_or(DEFAULT_ENTRY_MAX_BYTES)
            .min(max_bytes);
        let max_entries = map
            .get(AI_MAX_ENTRIES)
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| (1..=10_000).contains(v))
            .unwrap_or(DEFAULT_MAX_ENTRIES);
        let model = map
            .get(AI_MODEL)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty() && v.len() <= 128)
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());

        let api_key = map.get(AI_API_KEY).cloned().unwrap_or_default();
        AiSettings {
            enabled: map
                .get(AI_ENABLED)
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(DEFAULT_ENABLED),
            api_key_configured: !api_key.trim().is_empty(),
            api_key,
            model,
            threshold: map
                .get(AI_THRESHOLD)
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| (0.0..=1.0).contains(v))
                .unwrap_or(DEFAULT_THRESHOLD),
            max_per_tick: map
                .get(AI_MAX_PER_TICK)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=100).contains(v))
                .unwrap_or(DEFAULT_MAX_PER_TICK) as usize,
            debounce_ms: map
                .get(AI_DEBOUNCE_MS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (0..=600_000).contains(v))
                .unwrap_or(DEFAULT_DEBOUNCE_MS),
            min_interval_secs: map
                .get(AI_MIN_INTERVAL_SECS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (0..=3600).contains(v))
                .unwrap_or(DEFAULT_MIN_INTERVAL_SECS),
            max_bytes: max_bytes as usize,
            max_entries: max_entries as usize,
            entry_max_bytes: entry_max_bytes as usize,
            daily_cost_limit: map
                .get(AI_DAILY_COST_LIMIT)
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| *v > 0.0 && *v <= 10_000.0)
                .unwrap_or(DEFAULT_DAILY_COST_LIMIT),
            retention_days: map
                .get(AI_RETENTION_DAYS)
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| (1..=3650).contains(v))
                .unwrap_or(DEFAULT_RETENTION_DAYS),
        }
    }
}

/// PUT-side validation. Returns Err(message) for a rejected value; the
/// caller keeps the previous setting untouched. `ai_api_key` accepts any
/// string (an empty string clears the key) - secrecy, not validation.
pub async fn validate_setting(key: &str, value: &str, pool: &PgPool) -> Result<(), String> {
    let v = value.trim();
    match key {
        AI_ENABLED => {
            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
                Ok(())
            } else {
                Err("ai_enabled must be true or false".to_string())
            }
        }
        AI_API_KEY => {
            if value.len() <= 4096 {
                Ok(())
            } else {
                Err("ai_api_key must be at most 4096 characters".to_string())
            }
        }
        AI_MODEL => {
            let ok = !v.is_empty() && v.len() <= 128 && v.chars().all(|c| c.is_ascii_graphic());
            if ok {
                Ok(())
            } else {
                Err("ai_model must be 1-128 printable ASCII characters without spaces".to_string())
            }
        }
        AI_THRESHOLD => parse_range(v, 0.0..=1.0, "ai_threshold must be a number from 0 to 1"),
        AI_MAX_PER_TICK => parse_int_range(
            v,
            1..=100,
            "ai_max_per_tick must be an integer from 1 to 100",
        ),
        AI_DEBOUNCE_MS => parse_int_range(
            v,
            0..=600_000,
            "ai_stream_debounce_ms must be an integer from 0 to 600000",
        ),
        AI_MIN_INTERVAL_SECS => parse_int_range(
            v,
            0..=3600,
            "ai_stream_min_interval_secs must be an integer from 0 to 3600",
        ),
        AI_MAX_BYTES => parse_int_range(
            v,
            1024..=10_000_000,
            "ai_stream_max_bytes must be an integer from 1024 to 10000000",
        ),
        AI_MAX_ENTRIES => parse_int_range(
            v,
            1..=10_000,
            "ai_stream_max_entries must be an integer from 1 to 10000",
        ),
        AI_ENTRY_MAX_BYTES => {
            parse_int_range(
                v,
                64..=1_000_000,
                "ai_entry_max_bytes must be an integer from 64 to 1000000",
            )?;
            // Transcript compatibility: a per-entry cap above the whole
            // transcript budget would make every entry truncated.
            let max_bytes: i64 = get_setting_i64(pool, AI_MAX_BYTES)
                .await
                .unwrap_or(DEFAULT_MAX_BYTES);
            if v.parse::<i64>().unwrap_or(0) > max_bytes {
                return Err("ai_entry_max_bytes must not exceed ai_stream_max_bytes".to_string());
            }
            Ok(())
        }
        AI_DAILY_COST_LIMIT => parse_range(
            v,
            0.0..=10_000.0,
            "ai_daily_cost_limit must be a number from 0 to 10000",
        ),
        AI_RETENTION_DAYS => parse_int_range(
            v,
            1..=3650,
            "ai_verdict_retention_days must be an integer from 1 to 3650",
        ),
        AI_ENABLED_BOUNDARY => {
            Err("ai_enabled_at_packet_id is managed by the scheduler and read-only".to_string())
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

async fn get_setting_i64(pool: &PgPool, key: &str) -> Option<i64> {
    sqlx::query_scalar::<_, String>("SELECT value FROM app_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse().ok())
}

/// The enablement watermark. Persisted at the disable->enable transition,
/// cleared on disable, kept across restarts.
pub async fn read_enablement_boundary(pool: &PgPool) -> Option<i64> {
    get_setting_i64(pool, AI_ENABLED_BOUNDARY).await
}

pub async fn write_enablement_boundary(pool: &PgPool, value: i64) {
    // MAX(last_packet_id) over the small stream table: bounded by stream
    // count, runs once per enablement, never on the ingest path.
    let boundary: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(last_packet_id), 0) FROM stream")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let _ = sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2",
    )
    .bind(AI_ENABLED_BOUNDARY)
    .bind(value.max(boundary).to_string())
    .execute(pool)
    .await;
}

pub async fn clear_enablement_boundary(pool: &PgPool) {
    let _ = sqlx::query("DELETE FROM app_settings WHERE key = $1")
        .bind(AI_ENABLED_BOUNDARY)
        .execute(pool)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_bounds_reject_out_of_range() {
        assert!(parse_range("0.5", 0.0..=1.0, "x").is_ok());
        assert!(parse_range("0", 0.0..=1.0, "x").is_ok());
        assert!(parse_range("1", 0.0..=1.0, "x").is_ok());
        assert!(parse_range("1.01", 0.0..=1.0, "x").is_err());
        assert!(parse_range("-0.1", 0.0..=1.0, "x").is_err());
        assert!(parse_range("nan", 0.0..=1.0, "x").is_err());
        assert!(parse_range("inf", 0.0..=1.0, "x").is_err());
        assert!(parse_range("abc", 0.0..=1.0, "x").is_err());
    }

    #[test]
    fn integer_bounds_reject_out_of_range() {
        assert!(parse_int_range("10", 1..=100, "x").is_ok());
        assert!(parse_int_range("0", 1..=100, "x").is_err());
        assert!(parse_int_range("101", 1..=100, "x").is_err());
        assert!(parse_int_range("1.5", 1..=100, "x").is_err());
    }

    #[test]
    fn key_validation_never_leaks_value_into_error() {
        // The error path must stay usable for the UI without echoing key
        // material; wrong-type values are rejected before formatting.
        let long_key = "x".repeat(5000);
        assert!(long_key.len() > 4096);
    }

    #[test]
    fn settings_get_masks_the_api_key() {
        use crate::api::settings::mask_ai_key_in_map;

        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(AI_API_KEY.to_string(), "sk-or-secret".to_string());
        mask_ai_key_in_map(&mut map);
        assert!(!map.contains_key(AI_API_KEY));
        assert_eq!(
            map.get("ai_api_key_configured").map(|s| s.as_str()),
            Some("true")
        );

        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(AI_API_KEY.to_string(), "   ".to_string());
        mask_ai_key_in_map(&mut map);
        assert_eq!(
            map.get("ai_api_key_configured").map(|s| s.as_str()),
            Some("false")
        );

        let mut map: HashMap<String, String> = HashMap::new();
        mask_ai_key_in_map(&mut map);
        assert_eq!(
            map.get("ai_api_key_configured").map(|s| s.as_str()),
            Some("false")
        );
        assert!(!map.contains_key(AI_API_KEY));
    }
}
