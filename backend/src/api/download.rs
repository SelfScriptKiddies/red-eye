//! Agent distribution endpoints.
//!
//! * `GET /api/agent/download` — the red-eye-agent binary itself.
//!   DELIBERATELY PUBLIC (no Identity): operators wget it straight onto
//!   service hosts before they have any credentials; leaking the binary
//!   leaks nothing (all secrets live in the run command below).
//! * `GET /api/agent/install-command` — ready-to-paste run command with
//!   REAL NATS credentials. Authorized only: this is the actual secret.
//!
//! The binary is baked into the backend image at build time (same commit
//! as the backend itself — no version drift by construction); point
//! `AGENT_BINARY_PATH` at a custom file to override.

use actix_identity::Identity;
use actix_web::{get, web, HttpResponse, Responder};
use serde_json::json;

/// Serve the agent binary. PUBLIC BY DESIGN and mounted OUTSIDE the /api
/// scope at `/download/agent` (next to /metrics — everything under /api
/// requires a session): operators wget it straight onto service hosts
/// before they have any credentials, and leaking the binary leaks nothing
/// (all secrets live in the run command endpoint below).
/// Streamed from disk via web::block so the actix worker doesn't block on
/// a ~15 MB read.
#[utoipa::path(get, path = "/download/agent", tag = "agent",
    responses(
        (status = 200, description = "red-eye-agent binary (attachment)"),
        (status = 404, description = "Binary not bundled into this image (AGENT_BINARY_PATH)")
    ))]
#[get("/download/agent")]
async fn download_agent_binary(
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    let path = config.agent_binary_path.clone();
    match web::block(move || std::fs::read(&path)).await {
        Ok(Ok(bytes)) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .insert_header((
                "Content-Disposition",
                "attachment; filename=\"red-eye-agent\"",
            ))
            .body(bytes),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => HttpResponse::NotFound().json(
            json!({
                "error": format!(
                    "agent binary not found at {} — image built without it; \
                     download from GitHub releases or rebuild the backend image",
                    config.agent_binary_path
                )
            }),
        ),
        Ok(Err(e)) => HttpResponse::InternalServerError()
            .json(json!({"error": format!("failed to read agent binary: {}", e)})),
        Err(e) => HttpResponse::InternalServerError()
            .json(json!({"error": format!("blocking read failed: {}", e)})),
    }
}

/// The run command with real NATS credentials. Authorized only.
///
/// Lives at `/api/install-command` (NOT under /api/agent/...): the agent
/// scope has a `/api/agent/{id}` route that would swallow any
/// `/api/agent/<literal>` path.
///
/// The NATS host is derived from the request's Host header: the operator's
/// browser reaches the frontend at the machine's real address, and compose
/// publishes NATS on 4222 of the same machine — so `<that host>:4222` is
/// the address an outside agent needs.
#[utoipa::path(get, path = "/api/install-command", tag = "agent",
    responses((status = 200, description = "Ready-to-paste agent run command with credentials")))]
#[get("/install-command")]
async fn get_install_command(
    _identity: Identity,
    request: actix_web::HttpRequest,
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    let host = request
        .headers()
        .get(actix_web::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("NATS_HOST")
        .split(':')
        .next()
        .unwrap_or("NATS_HOST")
        .to_string();

    let command = format!(
        "./red-eye-agent \\\n  --agent-id my-agent-1 \\\n  -i eth0 \\\n  -p 8080,31337 \\\n  --nats-host {host}:4222 \\\n  --nats-login {login} \\\n  --nats-password {password} \\\n  --machine-ip 0.0.0.0",
        host = host,
        login = config.nats_login,
        password = config.nats_password,
    );

    HttpResponse::Ok().json(json!({
        "command": command,
        "download_path": "/download/agent",
        // Same image, same commit — what the served binary reports via --version.
        "built_from": crate::version::FULL,
    }))
}

/// Root-level (public) routes — mounted OUTSIDE the /api scope, next to
/// /metrics. See the module docs for why the download must be public.
pub fn public_config(cfg: &mut web::ServiceConfig) {
    cfg.service(download_agent_binary);
}

/// Authorized routes inside the /api scope.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_install_command);
}

#[cfg(test)]
mod tests {
    /// The command must never leak into the public download route: the
    /// download handler has no Identity extractor, the command handler
    /// does. Source-level guard so a future refactor can't silently swap
    /// the two.
    #[test]
    fn command_requires_identity_download_does_not() {
        let src = include_str!("download.rs");
        let download = src
            .split("#[get(\"/download/agent\")]")
            .nth(1)
            .and_then(|s| s.split("#[get(\"/install-command\")]").next())
            .expect("both routes present");
        assert!(
            !download.contains("Identity"),
            "download route must stay public (no Identity extractor)"
        );
        assert!(
            src.split("#[get(\"/install-command\")]")
                .nth(1)
                .expect("install command route present")
                .contains("_identity: Identity"),
            "install-command route must keep the Identity extractor"
        );
    }
}
