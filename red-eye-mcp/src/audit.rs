use log::warn;

use crate::auth::AuthContext;

// Audit records: token id, session id, tool, target ids, expected
// revision, dry-run/mutation status, result, sanitized error. Payload
// contents and bearer tokens are never audit fields - arguments are
// summarized to integer targets only. get_selected_streams records the
// url the operator handed over, never the returned bundle.

/// Cap for a recorded url target.
const TARGET_MAX: usize = 2048;

fn sanitize(text: &str, max_chars: usize) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(max_chars)
        .collect()
}

fn summarize_args(args: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    for key in [
        "service_id",
        "filter_id",
        "agent_id",
        "ip",
        "stream_id",
        "enabled",
        "enable",
    ] {
        if let Some(v) = args.get(key) {
            parts.push(format!("{}={}", key, v));
        }
    }
    if let Some(rev) = args.get("expected_revision") {
        parts.push(format!("expected_revision={}", rev));
    }
    let dry = args
        .get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    parts.push(format!("dry_run={}", dry));
    parts.join(" ")
}

pub fn mutation_attempt(auth: &AuthContext, tool: &str, args: &serde_json::Value) {
    log::info!(
        target: "mcp_audit",
        "MCP_AUDIT tool={} token_id={} session={} {} phase=attempt",
        tool, auth.token_id, auth.session_id, summarize_args(args)
    );
}

pub fn mutation_result(auth: &AuthContext, tool: &str, result: &str, error: &str) {
    log::info!(
        target: "mcp_audit",
        "MCP_AUDIT tool={} token_id={} session={} result={} error={}",
        tool,
        auth.token_id,
        auth.session_id,
        result,
        sanitize(error, 200)
    );
}

pub fn read_result(auth: &AuthContext, tool: &str, target: &str, result: &str, error: &str) {
    log::info!(
        target: "mcp_audit",
        "MCP_AUDIT tool={} token_id={} session={} target={} result={} error={}",
        tool,
        auth.token_id,
        auth.session_id,
        sanitize(target, TARGET_MAX),
        result,
        sanitize(error, 200)
    );
}

pub fn mutation_denied(auth: &AuthContext, tool: &str, why: &str) {
    warn!(
        target: "mcp_audit",
        "MCP_AUDIT tool={} token_id={} session={} result=denied reason={}",
        tool, auth.token_id, auth.session_id, why
    );
}
