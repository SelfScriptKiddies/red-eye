use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;

// Transport middleware: every HTTP request to /mcp passes through the
// same chain BEFORE any MCP handling:
//   1. reject tokens in query strings (spec prohibition),
//   2. validate the bearer against the backend (checked on initialization
//      AND every call, so revocation bites immediately; backend outage
//      fails closed),
//   3. rate limit per token and per source address,
//   4. bound session count, body size and JSON depth,
//   5. publish the identity as a request extension the handler reads
//      (rmcp forwards request Parts into the RequestContext).
// No failure path opens the door; errors are generic JSON, never echoes.

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub token_id: i64,
    pub capability: String,
    pub session_id: String,
}

pub struct AppState {
    pub backend: Arc<crate::backend::BackendClient>,
    pub rate_token: RateLimiter,
    pub rate_addr: RateLimiter,
    pub sessions: SessionTracker,
    pub permits: Arc<Semaphore>,
}

use crate::state::{RateLimiter, SessionTracker, token_hash};

fn deny(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error": message}))).into_response()
}

pub async fn middleware(
    State(state): State<std::sync::Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Concurrent-call budget: a burst of slow SSE streams cannot starve
    // validation or the backend.
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return deny(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many concurrent MCP calls",
        );
    };

    // Tokens in query strings/URLs are prohibited outright.
    if let Some(q) = req.uri().query() {
        let lower = q.to_ascii_lowercase();
        if ["token=", "access_token=", "auth="]
            .iter()
            .any(|p| lower.contains(p))
        {
            return deny(
                StatusCode::BAD_REQUEST,
                "tokens in query strings are prohibited; use the Authorization header",
            );
        }
    }

    // Session admission: unknown sessions must pass initialize; known
    // ones refresh; a full tracker rejects new sessions with 429.
    let session_header = req
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .filter(|sid| {
            sid.len() <= 128 && sid.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        });
    if let Some(sid) = session_header
        && state.sessions.touch(sid) == Some(false)
    {
        return deny(StatusCode::TOO_MANY_REQUESTS, "session limit reached");
    }

    // Bearer validation (backend roundtrip, fail closed on outage).
    let Some(auth_header) = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return deny(
            StatusCode::UNAUTHORIZED,
            "missing Authorization bearer header",
        );
    };
    let Some(presented) = auth_header
        .strip_prefix("Bearer ")
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
    else {
        return deny(
            StatusCode::UNAUTHORIZED,
            "invalid Authorization header shape",
        );
    };

    let identity = match state.backend.authorize(presented).await {
        Ok(id) => id,
        Err(_) => {
            // Includes backend outages: fail closed, generic message, no
            // echo of whether the token exists.
            return deny(StatusCode::UNAUTHORIZED, "token validation failed");
        }
    };

    // Rate limits: per token id, then per source address.
    let addr = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    if state
        .rate_token
        .check(&token_hash(&identity.token_id.to_string()))
        != Some(true)
    {
        return deny(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded for token",
        );
    }
    if state.rate_addr.check(&addr) != Some(true) {
        return deny(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded for address",
        );
    }

    // Buffer the body once (DefaultBodyLimit caps size): JSON depth and
    // envelope sanity happen here so malformed JSON-RPC dies at the gate
    // without reaching the service.
    let (mut parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, crate::config::MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return deny(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
    };
    if !bytes.is_empty() {
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) => {
                if crate::tools::json_depth(&v) > crate::config::MAX_JSON_DEPTH {
                    return deny(StatusCode::BAD_REQUEST, "request body nesting too deep");
                }
            }
            Err(_) => return deny(StatusCode::BAD_REQUEST, "malformed JSON-RPC request"),
        }
    }

    let auth = AuthContext {
        token_id: identity.token_id,
        capability: identity.capability,
        session_id: parts
            .headers
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| {
                s.chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
                    .take(64)
                    .collect()
            })
            .unwrap_or_else(|| format!("anon-{}", identity.token_id)),
    };
    parts.extensions.insert(auth);

    let is_delete = parts.method == axum::http::Method::DELETE;
    let session_id_for_close: Option<String> = parts
        .headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;

    // Session teardown on DELETE (MCP session close) frees the slot.
    if is_delete && let Some(sid) = session_id_for_close {
        state.sessions.forget(&sid);
    }
    response
}
