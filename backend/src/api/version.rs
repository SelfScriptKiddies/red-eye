//! `GET /api/version` — build versions of every service in the stack, for
//! spotting version drift (backend / analyzer / frontend / deployed agents).
//!
//! Probing the analyzer and frontend costs a NATS request and an HTTP GET
//! respectively, so results are CACHED for 60s with single-flight refresh:
//! a burst of requests hits the cache, exactly one request re-probes, the
//! rest wait for it and read the fresh cache (one endpoint request can
//! never fan out into a probe storm). Agent versions ride the heartbeat —
//! free to read, served live, not cached.
//!
//! Version sources:
//! * backend  — compiled in (crate version + git sha, src/version.rs)
//! * analyzer — NATS request-reply on `service_info.analyzer`
//!   (traffic-analyzer/src/version.rs answers)
//! * frontend — `GET {frontend_url}/version.json`, a build-time artifact
//!   written by frontend/scripts/write-version.mjs and served by nginx
//! * agents   — `version` field of the heartbeat

use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, get, web};
use async_nats::Client;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};

use crate::api::agent::agent_service::AgentService;

/// How long a probe result stays fresh. Short enough to notice a redeploy
/// within a minute, long enough that hammering F5 costs nothing.
pub const CACHE_TTL: Duration = Duration::from_secs(60);
/// Per-probe timeout: a dead service must not hold the HTTP request (nor
/// the single-flight lock) for long.
const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// One service's version, or why it could not be fetched.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ServiceVersion {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ServiceVersion {
    fn ok(version: impl Into<String>, git_sha: impl Into<String>) -> Self {
        Self {
            version: Some(version.into()),
            git_sha: Some(git_sha.into()),
            error: None,
        }
    }

    fn err(message: impl Into<String>) -> Self {
        Self {
            version: None,
            git_sha: None,
            error: Some(message.into()),
        }
    }
}

/// Wire shape of the analyzer's reply (traffic-analyzer/src/version.rs
/// `reply_payload` — locked by tests on both sides).
#[derive(Debug, Deserialize)]
struct AnalyzerVersionReply {
    #[allow(dead_code)]
    service: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    git_sha: Option<String>,
}

/// Wire shape of the frontend's version.json
/// (frontend/scripts/write-version.mjs).
#[derive(Debug, Deserialize)]
struct FrontendVersionReply {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    git_sha: Option<String>,
}

#[derive(Clone)]
struct CachedServices {
    fetched_at: Instant,
    analyzer: ServiceVersion,
    frontend: ServiceVersion,
}

pub struct VersionService {
    nats: Client,
    http: reqwest::Client,
    /// Base URL of the frontend (nginx serving dist/), e.g. "http://frontend:80".
    frontend_url: String,
    /// NATS subject the analyzer's version responder listens on.
    analyzer_subject: String,
    cache: RwLock<Option<CachedServices>>,
    /// Single-flight: while one request re-probes, the others queue here and
    /// then read the fresh cache instead of piling on.
    refresh_lock: Mutex<()>,
}

impl VersionService {
    pub fn new(nats: Client, frontend_url: String, analyzer_subject: String) -> Self {
        Self {
            nats,
            http: reqwest::Client::builder().build().unwrap_or_default(),
            frontend_url,
            analyzer_subject,
            cache: RwLock::new(None),
            refresh_lock: Mutex::new(()),
        }
    }

    async fn probe_analyzer(&self) -> ServiceVersion {
        // async-nats' per-client request timeout defaults to 10s — too long
        // to hold an HTTP request (and the single-flight lock), so the
        // outer tokio timeout caps the wait at PROBE_TIMEOUT.
        let request = self
            .nats
            .request(self.analyzer_subject.clone(), Vec::<u8>::new().into());
        match tokio::time::timeout(PROBE_TIMEOUT, request).await {
            Ok(Ok(reply)) => match serde_json::from_slice::<AnalyzerVersionReply>(&reply.payload) {
                Ok(parsed) => match (parsed.version, parsed.git_sha) {
                    (Some(v), Some(sha)) => ServiceVersion::ok(v, sha),
                    _ => ServiceVersion::err("analyzer reply missing version/git_sha"),
                },
                Err(e) => ServiceVersion::err(format!("malformed analyzer reply: {}", e)),
            },
            Ok(Err(e)) => ServiceVersion::err(format!("analyzer unreachable: {}", e)),
            Err(_) => ServiceVersion::err("analyzer request timed out"),
        }
    }

    async fn probe_frontend(&self) -> ServiceVersion {
        let url = format!("{}/version.json", self.frontend_url.trim_end_matches('/'));
        match self.http.get(&url).timeout(PROBE_TIMEOUT).send().await {
            Ok(response) if response.status().is_success() => match response
                .json::<FrontendVersionReply>()
                .await
            {
                Ok(parsed) => match (parsed.version, parsed.git_sha) {
                    (Some(v), Some(sha)) => ServiceVersion::ok(v, sha),
                    _ => ServiceVersion::err("frontend version.json missing fields"),
                },
                Err(e) => ServiceVersion::err(format!("malformed frontend version.json: {}", e)),
            },
            Ok(response) => {
                ServiceVersion::err(format!("frontend returned HTTP {}", response.status()))
            }
            Err(e) => ServiceVersion::err(format!("frontend unreachable: {}", e)),
        }
    }

    /// Cached (or freshly probed) analyzer/frontend versions. TTL-bounded
    /// and single-flight — see the module docs.
    async fn services(&self) -> CachedServices {
        // Fast path: fresh cache.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache
                .as_ref()
                .filter(|c| c.fetched_at.elapsed() < CACHE_TTL)
            {
                return cached.clone();
            }
        }

        // Slow path: one request refreshes, the rest wait and re-read.
        let _guard = self.refresh_lock.lock().await;
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache
                .as_ref()
                .filter(|c| c.fetched_at.elapsed() < CACHE_TTL)
            {
                return cached.clone();
            }
        }

        let (analyzer, frontend) = tokio::join!(self.probe_analyzer(), self.probe_frontend());
        let fresh = CachedServices {
            fetched_at: Instant::now(),
            analyzer,
            frontend,
        };
        *self.cache.write().await = Some(fresh.clone());
        fresh
    }
}

#[utoipa::path(get, path = "/api/version", tag = "version",
    responses((status = 200, description = "Build versions of stack services (cached 60s)")))]
#[get("/version")]
async fn get_versions(
    _identity: Identity,
    version_service: web::Data<Arc<VersionService>>,
    agent_service: web::Data<Arc<AgentService>>,
) -> impl Responder {
    let cached = version_service.services().await;

    // Agents ride the in-memory heartbeat map — always current, no probe.
    let agents: Vec<serde_json::Value> = agent_service
        .list_agents()
        .await
        .into_iter()
        .map(|a| {
            serde_json::json!({
                "agent_id": a.agent_id,
                "online": a.online,
                "version": a.version,
            })
        })
        .collect();

    HttpResponse::Ok().json(serde_json::json!({
        "backend": {
            "version": crate::version::VERSION,
            "git_sha": crate::version::GIT_SHA,
        },
        "analyzer": cached.analyzer,
        "frontend": cached.frontend,
        "agents": agents,
        "cache": {
            "ttl_secs": CACHE_TTL.as_secs(),
            "fetched_at_unix": fresh_unix_ms(&cached.fetched_at),
            "age_secs": cached.fetched_at.elapsed().as_secs(),
        },
    }))
}

fn fresh_unix_ms(at: &Instant) -> u64 {
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    now_unix.saturating_sub(at.elapsed().as_millis() as u64)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_versions);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror of the analyzer's reply payload (locked by a test on the
    /// analyzer side too) — the backend must parse what it actually sends.
    #[test]
    fn parses_analyzer_reply() {
        let wire = r#"{"service":"traffic-analyzer","version":"0.1.0","git_sha":"df78fe2"}"#;
        let parsed: AnalyzerVersionReply = serde_json::from_str(wire).unwrap();
        assert_eq!(parsed.version.as_deref(), Some("0.1.0"));
        assert_eq!(parsed.git_sha.as_deref(), Some("df78fe2"));
    }

    /// Mirror of frontend/scripts/write-version.mjs output.
    #[test]
    fn parses_frontend_version_json() {
        let wire = r#"{"service":"frontend","version":"0.0.0","git_sha":"abc1234","built_at":"2026-10-23T00:00:00Z"}"#;
        let parsed: FrontendVersionReply = serde_json::from_str(wire).unwrap();
        assert_eq!(parsed.version.as_deref(), Some("0.0.0"));
        assert_eq!(parsed.git_sha.as_deref(), Some("abc1234"));
    }

    #[test]
    fn service_version_serializes_minimal_json() {
        let ok = ServiceVersion::ok("0.1.0", "df78fe2");
        let json = serde_json::to_value(&ok).unwrap();
        assert_eq!(json["version"], "0.1.0");
        assert!(json.get("error").is_none() || json["error"].is_null());

        let err = ServiceVersion::err("unreachable");
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["error"], "unreachable");
        assert!(json.get("version").is_none() || json["version"].is_null());
    }

    /// A fresh cache entry must read as fresh, an aged one as stale —
    /// the entire overload protection hangs off this comparison.
    #[test]
    fn cache_ttl_bounds_freshness() {
        let fresh = CachedServices {
            fetched_at: Instant::now(),
            analyzer: ServiceVersion::ok("0", "0"),
            frontend: ServiceVersion::ok("0", "0"),
        };
        assert!(fresh.fetched_at.elapsed() < CACHE_TTL);

        let aged = CachedServices {
            fetched_at: Instant::now() - CACHE_TTL - Duration::from_secs(1),
            analyzer: ServiceVersion::ok("0", "0"),
            frontend: ServiceVersion::ok("0", "0"),
        };
        assert!(aged.fetched_at.elapsed() >= CACHE_TTL);
    }
}
