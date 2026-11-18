use actix_web::HttpRequest;
use actix_web::HttpResponse;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

// Bearer credentials for internal service callers (soc-worker,
// red-eye-mcp). Unlike browser sessions (Identity cookies) these are
// backend-generated opaque tokens stored as SHA-256 hashes in
// app_settings; GET never returns them. A missing or mismatched token
// fails closed with 401, a backend/store outage with 5xx - never open.

const TOKEN_CHARS: usize = 48;

pub fn extract_bearer(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get(actix_web::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

pub fn token_hash(presented: &str) -> String {
    let mut h = Sha256::new();
    h.update(presented.as_bytes());
    hex_encode(&h.finalize())
}

/// Constant-time equality over the hex digests; lengths differ only for
/// corrupted stores, where early exit leaks nothing useful.
fn hashes_equal(a: &str, b: &str) -> bool {
    if a.len() != b.len() || a.is_empty() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 48 hex chars from the OS CSPRNG. No dependency: the backend only ever
/// generates these on an operator's explicit action.
pub fn generate_token() -> std::io::Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; TOKEN_CHARS / 2];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(hex_encode(&bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Validate a presented bearer against the stored hash. Ok(true) only
/// when the credential exists and matches; store failures bubble up so
/// callers return 5xx (fail closed) instead of 401-on-outage ambiguity.
pub async fn validate_service_token(
    pool: &PgPool,
    setting_key: &str,
    presented: &str,
) -> Result<bool, sqlx::Error> {
    let stored: Option<String> =
        sqlx::query_scalar("SELECT value FROM app_settings WHERE key = $1")
            .bind(setting_key)
            .fetch_optional(pool)
            .await?;
    let Some(stored) = stored else {
        return Ok(false);
    };
    Ok(hashes_equal(stored.trim(), &token_hash(presented)))
}

/// Throttled last-seen write: at most one UPDATE per minute per
/// credential, so a busy worker or MCP proxy cannot turn every request
/// into a settings-row write.
pub async fn touch_last_seen(pool: &PgPool, key: &str) {
    let now = unix_now();
    let last: Option<String> = sqlx::query_scalar("SELECT value FROM app_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
    let seen_recently = last
        .and_then(|v| v.trim().parse::<i64>().ok())
        .map(|v| now - (v as f64) < 60.0)
        .unwrap_or(false);
    if seen_recently {
        return;
    }
    let _ = sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2",
    )
    .bind(key)
    .bind(format!("{}", now as i64))
    .execute(pool)
    .await;
}

/// Guard for service endpoints: 401 on a missing/unknown bearer, 5xx
/// passthrough on store failure. Never allows through on error.
pub async fn require_service_token(
    pool: &PgPool,
    req: &HttpRequest,
    setting_key: &str,
    last_seen_key: Option<&str>,
) -> Result<(), HttpResponse> {
    let Some(presented) = extract_bearer(req) else {
        return Err(HttpResponse::Unauthorized()
            .json(serde_json::json!({"error": "missing bearer credential"})));
    };
    match validate_service_token(pool, setting_key, &presented).await {
        Ok(true) => {
            if let Some(k) = last_seen_key {
                touch_last_seen(pool, k).await;
            }
            Ok(())
        }
        Ok(false) => Err(HttpResponse::Unauthorized()
            .json(serde_json::json!({"error": "invalid bearer credential"}))),
        Err(e) => Err(crate::api::sqlx_error_response(&e)),
    }
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_hex_sha256() {
        let h1 = token_hash("abc");
        let h2 = token_hash("abc");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
        assert_ne!(token_hash("abd"), h1);
    }

    #[test]
    fn hashes_equal_is_length_safe() {
        assert!(hashes_equal("aa", "aa"));
        assert!(!hashes_equal("aa", "aaa"));
        assert!(!hashes_equal("", ""));
        assert!(!hashes_equal("ab", "ba"));
    }

    #[test]
    fn generated_tokens_have_the_expected_shape() {
        let t = generate_token().unwrap();
        assert_eq!(t.len(), TOKEN_CHARS);
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(generate_token().unwrap(), t);
    }

    #[test]
    fn bearer_extraction_is_strict() {
        let req = actix_web::test::TestRequest::default()
            .insert_header(("Authorization", "Bearer tok-1"))
            .to_http_request();
        assert_eq!(extract_bearer(&req).as_deref(), Some("tok-1"));
        let bare = actix_web::test::TestRequest::default()
            .insert_header(("Authorization", "tok-1"))
            .to_http_request();
        assert!(extract_bearer(&bare).is_none());
        let none = actix_web::test::TestRequest::default().to_http_request();
        assert!(extract_bearer(&none).is_none());
    }
}
