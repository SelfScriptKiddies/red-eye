use serde_json::{Value, json};
use std::collections::HashMap;

use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, Implementation,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt, RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

use crate::backend::JobError;

// Read-only MCP client. One short-lived session per job attempt: no
// cross-job state, no payload caching, and an auth header on every call.

pub struct McpSession {
    client: RunningService<RoleClient, ClientConfig>,
}

/// The exact preliminary-step allowlist from the fixed playbook.
pub const PRELIMINARY_ALLOWLIST: [&str; 4] = [
    "get_packet",
    "get_message_headers",
    "get_stream_tags",
    "list_related_streams",
];

impl McpSession {
    pub async fn connect(mcp_url: &str, token: &str) -> Result<McpSession, JobError> {
        let config = StreamableHttpClientTransportConfig::with_uri(mcp_url)
            .auth_header(format!("Bearer {}", token));
        let transport = StreamableHttpClientTransport::from_config(config);
        let client_info = ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::new("soc-worker", env!("CARGO_PKG_VERSION")),
        );
        let client = client_info
            .serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Auto {
                    preferred_versions: vec![rmcp::model::ProtocolVersion::V_2025_11_25],
                    legacy_version: Some(rmcp::model::ProtocolVersion::V_2025_06_18),
                },
            )
            .await
            .map_err(|e| JobError::Transient(format!("mcp initialize: {}", e)))?;
        Ok(McpSession { client })
    }

    /// One tool call. Returns the parsed JSON payload (tool results carry
    /// the backend JSON as text content) or a typed failure.
    pub async fn call(&self, name: &str, args: Value) -> Result<Value, JobError> {
        let map: serde_json::Map<String, Value> = args.as_object().cloned().unwrap_or_default();
        let params = CallToolRequestParams::new(name.to_string()).with_arguments(map);
        let result: CallToolResult = self
            .client
            .call_tool(params)
            .await
            .map_err(|e| JobError::Transient(format!("mcp call {}: {}", name, e)))?;
        if result.is_error == Some(true) {
            return Err(JobError::Transient(format!(
                "mcp tool {} reported error",
                name
            )));
        }
        let text = result
            .content
            .first()
            .and_then(|c| match c {
                rmcp::model::ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                JobError::Permanent(format!("mcp tool {} returned no text content", name))
            })?;
        serde_json::from_str(&text).map_err(|_| {
            JobError::Permanent(format!("mcp tool {} returned non-JSON payload", name))
        })
    }

    pub async fn close(self) {
        let _ = self.client.cancel().await;
    }
}

/// Validate one preliminary evidence request against the allowlist:
/// exact tool name, permitted argument shape, ids present as integers.
pub fn validate_preliminary_request(req: &Value) -> Result<(String, Value), String> {
    let tool = req
        .get("tool")
        .and_then(|v| v.as_str())
        .ok_or("evidence request missing 'tool'")?;
    if !PRELIMINARY_ALLOWLIST.contains(&tool) {
        return Err(format!(
            "tool '{}' is not in the preliminary allowlist",
            tool
        ));
    }
    let mut args = HashMap::new();
    match tool {
        "get_packet" => {
            let id = req
                .get("packet_id")
                .and_then(|v| v.as_i64())
                .ok_or("get_packet needs integer packet_id")?;
            args.insert("packet_id".to_string(), json!(id));
        }
        "get_message_headers" => {
            let id = req
                .get("message_id")
                .and_then(|v| v.as_i64())
                .ok_or("get_message_headers needs integer message_id")?;
            args.insert("message_id".to_string(), json!(id));
        }
        "get_stream_tags" => {
            let id = req
                .get("stream_id")
                .and_then(|v| v.as_i64())
                .ok_or("get_stream_tags needs integer stream_id")?;
            args.insert("stream_id".to_string(), json!(id));
        }
        "list_related_streams" => {
            let id = req
                .get("stream_id")
                .and_then(|v| v.as_i64())
                .ok_or("list_related_streams needs integer stream_id")?;
            let window = req
                .get("window_secs")
                .and_then(|v| v.as_i64())
                .unwrap_or(300);
            args.insert("stream_id".to_string(), json!(id));
            args.insert("window_secs".to_string(), json!(window.clamp(1, 3600)));
        }
        _ => unreachable!("allowlist checked above"),
    }
    Ok((tool.to_string(), Value::Object(args.into_iter().collect())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn allowlist_rejects_unknown_and_shapes_args() {
        assert!(validate_preliminary_request(&json!({"tool": "get_stream_summary"})).is_err());
        let (t, a) =
            validate_preliminary_request(&json!({"tool": "get_packet", "packet_id": 5})).unwrap();
        assert_eq!(t, "get_packet");
        assert_eq!(a["packet_id"], 5);
        let (t, a) = validate_preliminary_request(
            &json!({"tool": "list_related_streams", "stream_id": 5, "window_secs": 99999}),
        )
        .unwrap();
        assert_eq!(t, "list_related_streams");
        assert_eq!(a["window_secs"], 3600);
        // A request with an extra injected argument loses it.
        let (_, a) = validate_preliminary_request(
            &json!({"tool": "get_packet", "packet_id": 5, "evil": true}),
        )
        .unwrap();
        assert!(a.get("evil").is_none());
    }

    #[test]
    fn missing_ids_are_rejected() {
        assert!(validate_preliminary_request(&json!({"tool": "get_packet"})).is_err());
        assert!(validate_preliminary_request(&json!({})).is_err());
    }
}
