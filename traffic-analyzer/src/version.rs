//! Compile-time version stamping + the NATS version responder.
//!
//! The responder is what the backend's `/api/version` probes: it subscribes
//! to `service_info.analyzer` and answers every request with a JSON payload
//! carrying this build's version and git sha (see backend/src/api/version.rs
//! — the reply shape must stay in sync with `AnalyzerVersionReply` there).

use async_nats::Client;
use futures::StreamExt;
use log::{error, info};
use serde_json::json;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_SHA: &str = env!("RED_EYE_GIT_SHA");
pub const FULL: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("RED_EYE_GIT_SHA"));

/// The exact bytes the backend expects as a reply (locked by tests on both
/// sides): `{"service":"traffic-analyzer","version":..,"git_sha":..}`.
pub fn reply_payload() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "service": "traffic-analyzer",
        "version": VERSION,
        "git_sha": GIT_SHA,
    }))
    .expect("version reply is always serializable")
}

/// Subscribe to `subject` and answer each request with the version payload.
/// Garbage/foreign messages are simply answered too (the payload is
/// constant and harmless) — only requests WITHOUT a reply subject are
/// dropped.
pub fn spawn_version_responder(client: Client, subject: String) {
    tokio::spawn(async move {
        let mut subscriber = match client.subscribe(subject.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to {} for version requests: {}", subject, e);
                return;
            }
        };
        info!("Listening for version requests on {}", subject);

        while let Some(message) = subscriber.next().await {
            let Some(reply) = message.reply.clone() else {
                continue;
            };
            if let Err(e) = client.publish(reply, reply_payload().into()).await {
                error!("Failed to publish version reply: {}", e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lock the wire shape the backend parses (backend/src/api/version.rs
    /// `parses_analyzer_reply` mirrors this from the other side).
    #[test]
    fn reply_payload_matches_backend_contract() {
        let v: serde_json::Value = serde_json::from_slice(&reply_payload()).unwrap();
        assert_eq!(v["service"], "traffic-analyzer");
        assert_eq!(v["version"], VERSION);
        assert_eq!(v["git_sha"], GIT_SHA);
    }

    #[test]
    fn full_version_combines_version_and_sha() {
        assert_eq!(FULL, format!("{}+{}", VERSION, GIT_SHA));
        assert!(!GIT_SHA.is_empty());
    }
}
