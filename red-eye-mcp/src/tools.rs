use serde_json::{Value, json};

use crate::backend::BackendClient;
use crate::state::{ConsumeError, DryRunStore};

// Tool catalog: names, descriptions, JSON schemas and class. Read-only
// tools are always listed in read_only mode; mutation tools exist only in
// a full_control process AND are listed only to full_control tokens. The
// dispatcher validates arguments against these bounds server-side.

pub const TOOL_LIST_LIMIT_MAX: u64 = 100;

fn limit_schema_value() -> serde_json::Value {
    json!({"type": "integer", "minimum": 1, "maximum": TOOL_LIST_LIMIT_MAX, "default": 20})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolClass {
    /// Always listed: bounded backend reads, no process opt-in needed.
    ReadOnly,
    Mutation,
    /// Listed and callable only when the process opted in via config
    /// (fetch_url): network egress is not part of the default surface.
    Fetch,
}

pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub class: ToolClass,
    pub schema: Value,
}

pub fn catalog() -> Vec<ToolDef> {
    let limit = limit_schema_value();
    let limit_schema = |dflt: i64| {
        let mut v = limit.clone();
        v["default"] = json!(dflt);
        v
    };
    vec![
        ToolDef {
            name: "list_services",
            description: "List monitored services (id, name, port, protocol, assignment mode). No payloads.",
            class: ToolClass::ReadOnly,
            schema: json!({"type": "object", "properties": {}}),
        },
        ToolDef {
            name: "list_agents",
            description: "List capture agents with online state and machine IP.",
            class: ToolClass::ReadOnly,
            schema: json!({"type": "object", "properties": {}}),
        },
        ToolDef {
            name: "search_streams",
            description: "Keyset-paginated stream search with bounded service, agent, tag, favorite, IP, protocol and time filters. Returns summaries, not payloads.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "service_id": {"type": "integer"},
                    "agent_id": {"type": "string", "maxLength": 255},
                    "tag_id": {"type": "integer"},
                    "favorite": {"type": "boolean"},
                    "ip": {"type": "string", "maxLength": 45, "description": "address prefix to match src or dst"},
                    "protocol": {"type": "string", "enum": ["TCP", "UDP"]},
                    "from_secs": {"type": "integer"},
                    "to_secs": {"type": "integer"},
                    "limit": limit_schema(20),
                    "cursor_time": {"type": "integer"},
                    "cursor_id": {"type": "integer"}
                }
            }),
        },
        ToolDef {
            name: "get_stream_summary",
            description: "Tuple, service, agent, counts, tags, favorite, segment metadata plus latest JEV verdict and triage state for one stream. No payloads.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {"stream_id": {"type": "integer"}},
                "required": ["stream_id"]
            }),
        },
        ToolDef {
            name: "get_stream_messages",
            description: "Bounded chronological packet and L7 message excerpts for one stream, with direction, ids, times and explicit truncation marks.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "stream_id": {"type": "integer"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                    "max_entry_bytes": {"type": "integer", "minimum": 128, "maximum": 16384, "default": 4096}
                },
                "required": ["stream_id"]
            }),
        },
        ToolDef {
            name: "get_packet",
            description: "One known packet with bounded text, hexadecimal and base64 representations.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "packet_id": {"type": "integer"},
                    "max_bytes": {"type": "integer", "minimum": 128, "maximum": 65536, "default": 8192}
                },
                "required": ["packet_id"]
            }),
        },
        ToolDef {
            name: "get_message_headers",
            description: "One known HTTP message's headers, byte-capped.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "message_id": {"type": "integer"},
                    "max_bytes": {"type": "integer", "minimum": 128, "maximum": 16384, "default": 4096}
                },
                "required": ["message_id"]
            }),
        },
        ToolDef {
            name: "get_stream_tags",
            description: "Tags attached to one stream.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {"stream_id": {"type": "integer"}},
                "required": ["stream_id"]
            }),
        },
        ToolDef {
            name: "get_jev_verdicts",
            description: "JEV analysis verdicts for one stream: typed answers and revisions, never provider credentials.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "stream_id": {"type": "integer"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}
                },
                "required": ["stream_id"]
            }),
        },
        ToolDef {
            name: "list_related_streams",
            description: "Streams sharing the same remote IP, service and agent within a required bounded window (max 3600s). Summaries only.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {
                    "stream_id": {"type": "integer"},
                    "window_secs": {"type": "integer", "minimum": 1, "maximum": 3600, "default": 300},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 20}
                },
                "required": ["stream_id"]
            }),
        },
        ToolDef {
            name: "get_soc_result",
            description: "One validated LLM-SOC triage result by id. No hidden reasoning exists.",
            class: ToolClass::ReadOnly,
            schema: json!({
                "type": "object",
                "properties": {"result_id": {"type": "integer"}},
                "required": ["result_id"]
            }),
        },
        ToolDef {
            name: "get_component_health",
            description: "Operator-safe backend component status without secrets.",
            class: ToolClass::ReadOnly,
            schema: json!({"type": "object", "properties": {}}),
        },
        // ----- Fetch: exists only when the deployment set
        // MCP_ENABLE_FETCH_URL; the handler treats it as unknown otherwise.
        ToolDef {
            name: "fetch_url",
            description: "Fetch a public http(s) URL and return extracted readable text \
                          (title, description, body with inline links). Private networks, \
                          link-local and other special-purpose addresses are blocked; \
                          redirects are validated hop by hop; output and download sizes \
                          are capped. This tool causes network egress and exists only if \
                          the deployment opted in via MCP_ENABLE_FETCH_URL (disabled by default).",
            class: ToolClass::Fetch,
            schema: json!({
                "type": "object",
                "properties": {
                    "url": {"type": "string", "maxLength": 2048, "description": "absolute http(s) URL of a public host"}
                },
                "required": ["url"]
            }),
        },
        // ----- Full-control mutations: listed only for full_control
        // tokens in a full_control process; the backend re-checks the
        // service credential scope independently. -----
        ToolDef {
            name: "create_service",
            description: "Create a monitored service. Requires expected_revision, confirm=true, reason. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string", "maxLength": 255},
                    "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "protocol": {"type": "string", "enum": ["TCP", "UDP"]},
                    "is_global": {"type": "boolean", "default": true},
                    "agent_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 100},
                    "http_parse": {"type": "boolean"},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500}
                },
                "required": ["name", "port", "protocol", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "update_service",
            description: "Update an existing service (options and agent assignment). Requires expected_revision, confirm=true, reason. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "service_id": {"type": "integer"},
                    "name": {"type": "string", "maxLength": 255},
                    "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "protocol": {"type": "string", "enum": ["TCP", "UDP"]},
                    "is_global": {"type": "boolean"},
                    "agent_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 100},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500}
                },
                "required": ["service_id", "name", "port", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "delete_service",
            description: "Delete a monitored service. Supports dry_run. Requires expected_revision, confirm=true, reason, dry_run_op_id for the mutation. full_control only.",
            class: ToolClass::Mutation,
            schema: mutation_dry_schema("service_id"),
        },
        ToolDef {
            name: "set_service_agents",
            description: "Replace the explicit agent set of a non-global service. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "service_id": {"type": "integer"},
                    "agent_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 100},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500},
                    "dry_run": {"type": "boolean"},
                    "dry_run_op_id": {"type": "string", "maxLength": 64}
                },
                "required": ["service_id", "agent_ids", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "set_agent_network_settings",
            description: "Set server-side interface/machine-ip overrides for one agent. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "maxLength": 255},
                    "interface": {"type": "string", "maxLength": 64},
                    "machine_ip": {"type": "string", "maxLength": 45},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500},
                    "dry_run": {"type": "boolean"},
                    "dry_run_op_id": {"type": "string", "maxLength": 64}
                },
                "required": ["agent_id", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "set_proxy_protection",
            description: "Enable or disable harpoon proxy protection (traffic redirect) for a service on an agent. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "service_id": {"type": "integer"},
                    "agent_id": {"type": "string", "maxLength": 255},
                    "enable": {"type": "boolean"},
                    "tls_mitm": {"type": "boolean", "default": false},
                    "proxy_port": {"type": "integer", "minimum": 1024, "maximum": 65535},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500},
                    "dry_run": {"type": "boolean"},
                    "dry_run_op_id": {"type": "string", "maxLength": 64}
                },
                "required": ["service_id", "agent_id", "enable", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "create_filter",
            description: "Create a match filter. Requires expected_revision, confirm=true, reason. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string", "maxLength": 255},
                    "color": {"type": "integer", "minimum": 0, "maximum": 360},
                    "template_type": {"type": "string", "enum": ["regex", "substr", "bsubstr"]},
                    "template": {"type": "string", "maxLength": 255},
                    "direction": {"type": "string", "enum": ["in", "out", "both"]},
                    "special_type": {"type": "string", "enum": ["flag", "alert", "block", "block_conn", "favorite"]},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500}
                },
                "required": ["name", "template_type", "template", "direction", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "update_filter",
            description: "Update a filter's name, template, direction or type. Requires expected_revision, confirm=true, reason. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "filter_id": {"type": "integer"},
                    "name": {"type": "string", "maxLength": 255},
                    "color": {"type": "integer", "minimum": 0, "maximum": 360},
                    "template_type": {"type": "string", "enum": ["regex", "substr", "bsubstr"]},
                    "template": {"type": "string", "maxLength": 255},
                    "direction": {"type": "string", "enum": ["in", "out", "both"]},
                    "special_type": {"type": "string", "enum": ["flag", "alert", "block", "block_conn", "favorite"]},
                    "enabled": {"type": "boolean"},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500}
                },
                "required": ["filter_id", "name", "template_type", "template", "direction", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "delete_filter",
            description: "Delete a filter. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: mutation_dry_schema("filter_id"),
        },
        ToolDef {
            name: "set_filter_enabled",
            description: "Enable or disable a filter. Disabled filters stop being pushed to agents. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "filter_id": {"type": "integer"},
                    "enabled": {"type": "boolean"},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500},
                    "dry_run": {"type": "boolean"},
                    "dry_run_op_id": {"type": "string", "maxLength": 64}
                },
                "required": ["filter_id", "enabled", "expected_revision", "confirm", "reason"]
            }),
        },
        ToolDef {
            name: "remove_active_block",
            description: "Remove an active IP block (unban) on one agent or all agents. Supports dry_run. full_control only.",
            class: ToolClass::Mutation,
            schema: json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "maxLength": 255, "description": "agent id or _all"},
                    "ip": {"type": "string", "maxLength": 45},
                    "expected_revision": {"type": "integer"},
                    "confirm": {"type": "boolean"},
                    "reason": {"type": "string", "maxLength": 500},
                    "dry_run": {"type": "boolean"},
                    "dry_run_op_id": {"type": "string", "maxLength": 64}
                },
                "required": ["agent_id", "ip", "expected_revision", "confirm", "reason"]
            }),
        },
    ]
}

fn mutation_dry_schema(id_field: &str) -> Value {
    json!({
        "type": "object",
        "properties": {
            id_field: {"type": "integer"},
            "expected_revision": {"type": "integer"},
            "confirm": {"type": "boolean"},
            "reason": {"type": "string", "maxLength": 500},
            "dry_run": {"type": "boolean"},
            "dry_run_op_id": {"type": "string", "maxLength": 64}
        },
        "required": [id_field, "expected_revision", "confirm", "reason"]
    })
}

/// The exact intended-change description a dry-run returns and a mutation
/// must reference. Pure function of the arguments so the two calls agree.
pub fn intended_change(tool: &str, args: &Value) -> String {
    format!("{} {}", tool, canonical_args(tool, args))
}

fn canonical_args(tool: &str, args: &Value) -> String {
    let s = |k: &str| {
        args.get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let i = |k: &str| args.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
    let b = |k: &str| args.get(k).and_then(|v| v.as_bool()).unwrap_or(false);
    match tool {
        "delete_service" => format!(
            "service={} revision={}",
            i("service_id"),
            i("expected_revision")
        ),
        "delete_filter" => format!(
            "filter={} revision={}",
            i("filter_id"),
            i("expected_revision")
        ),
        "set_service_agents" => format!(
            "service={} agents={:?} revision={}",
            i("service_id"),
            args.get("agent_ids").cloned().unwrap_or(json!([])),
            i("expected_revision")
        ),
        "set_agent_network_settings" => format!(
            "agent={} interface={} machine_ip={} revision={}",
            s("agent_id"),
            s("interface"),
            s("machine_ip"),
            i("expected_revision")
        ),
        "set_proxy_protection" => format!(
            "service={} agent={} enable={} tls_mitm={} revision={}",
            i("service_id"),
            s("agent_id"),
            b("enable"),
            b("tls_mitm"),
            i("expected_revision")
        ),
        "set_filter_enabled" => format!(
            "filter={} enabled={} revision={}",
            i("filter_id"),
            b("enabled"),
            i("expected_revision")
        ),
        "remove_active_block" => format!(
            "agent={} ip={} revision={}",
            s("agent_id"),
            s("ip"),
            i("expected_revision")
        ),
        _ => String::new(),
    }
}

/// Dry-run registration: returns the operation payload the caller must
/// echo back on the mutation call.
pub fn dry_run(tool: &str, args: &Value, store: &DryRunStore) -> Result<Value, String> {
    let change = intended_change(tool, args);
    let op_id = store
        .register(change.clone())
        .ok_or_else(|| "dry-run operation store is full; retry shortly".to_string())?;
    Ok(json!({
        "dry_run": true,
        "operation_id": op_id,
        "intended_change": change,
        "note": "repeat the same call with confirm=true and dry_run_op_id set to execute; the operation expires shortly and must reference the same state",
    }))
}

/// Consume a dry-run op for a mutation; Err string on any mismatch.
pub fn require_dryrun_op(tool: &str, args: &Value, store: &DryRunStore) -> Result<(), String> {
    let op_id = args
        .get("dry_run_op_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .ok_or("this destructive operation requires dry_run=true first, then dry_run_op_id")?;
    let expected = intended_change(tool, args);
    match store.consume(op_id.trim(), &expected) {
        Some(Ok(_)) => Ok(()),
        Some(Err(ConsumeError::Unknown)) => {
            Err("unknown dry_run_op_id; run the dry-run again".to_string())
        }
        Some(Err(ConsumeError::Expired)) => {
            Err("dry_run_op_id expired; run the dry-run again".to_string())
        }
        Some(Err(ConsumeError::Drift)) => {
            Err("arguments changed since the dry-run; run the dry-run again".to_string())
        }
        None => Err("dry-run store unavailable".to_string()),
    }
}

/// Which destructive tools need the dry-run chain.
pub fn needs_dryrun(tool: &str) -> bool {
    matches!(
        tool,
        "delete_service"
            | "delete_filter"
            | "set_service_agents"
            | "set_agent_network_settings"
            | "set_proxy_protection"
            | "set_filter_enabled"
            | "remove_active_block"
    )
}

/// Opt-in dimension of the listing/dispatch gate: Fetch tools are unknown
/// unless the process opted in; every other class passes this check and
/// remains subject to its own capability rules in the handler.
pub fn opt_in_allowed(class: ToolClass, fetch_url_enabled: bool) -> bool {
    match class {
        ToolClass::ReadOnly | ToolClass::Mutation => true,
        ToolClass::Fetch => fetch_url_enabled,
    }
}

/// Argument sanity shared by the dispatcher: depth-bounded object, string
/// values within reason, total serialized size capped. The backend
/// applies the per-field bounds; this keeps untrusted input small before
/// it ever leaves the process.
pub fn validate_args(args: Option<&serde_json::Map<String, Value>>) -> Result<Value, String> {
    let value = Value::Object(args.cloned().unwrap_or_default());
    let serialized = value.to_string();
    if serialized.len() > 16 * 1024 {
        return Err("tool arguments exceed 16 KiB".to_string());
    }
    if json_depth(&value) > crate::config::MAX_JSON_DEPTH {
        return Err("tool arguments nesting too deep".to_string());
    }
    Ok(value)
}

pub fn json_depth(v: &Value) -> usize {
    match v {
        Value::Object(m) => 1 + m.values().map(json_depth).max().unwrap_or(0),
        Value::Array(a) => 1 + a.iter().map(json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

pub async fn dispatch_read(
    backend: &BackendClient,
    tool: &str,
    args: &Value,
) -> Result<Value, String> {
    let int_arg = |k: &str| -> Result<i64, String> {
        args.get(k)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| format!("missing integer argument '{}'", k))
    };
    match tool {
        "list_services" => wrap(backend.list_services().await),
        "list_agents" => wrap(backend.list_agents().await),
        "search_streams" => wrap(backend.search_streams(args).await),
        "get_stream_summary" => wrap(backend.stream_summary(int_arg("stream_id")?).await),
        "get_stream_messages" => wrap(
            backend
                .stream_messages(
                    int_arg("stream_id")?,
                    args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20),
                    args.get("max_entry_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4096),
                )
                .await,
        ),
        "get_packet" => wrap(
            backend
                .get_packet(
                    int_arg("packet_id")?,
                    args.get("max_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(8192),
                )
                .await,
        ),
        "get_message_headers" => wrap(
            backend
                .get_message_headers(
                    int_arg("message_id")?,
                    args.get("max_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4096),
                )
                .await,
        ),
        "get_stream_tags" => wrap(backend.stream_tags(int_arg("stream_id")?).await),
        "get_jev_verdicts" => wrap(
            backend
                .stream_jev_verdicts(
                    int_arg("stream_id")?,
                    args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20),
                )
                .await,
        ),
        "list_related_streams" => wrap(
            backend
                .related_streams(
                    int_arg("stream_id")?,
                    args.get("window_secs")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(300),
                    args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20),
                )
                .await,
        ),
        "get_soc_result" => wrap(backend.get_soc_result(int_arg("result_id")?).await),
        "get_component_health" => wrap(backend.component_health().await),
        _ => Err(format!("unknown tool '{}'", tool)),
    }
}

fn wrap(r: Result<Value, crate::backend::BackendError>) -> Result<Value, String> {
    match r {
        Ok(v) => Ok(v),
        Err(crate::backend::BackendError::NotFound) => Err("not found".to_string()),
        Err(crate::backend::BackendError::Rejected { status, message }) => {
            Err(format!("rejected by backend ({}): {}", status, message))
        }
        Err(crate::backend::BackendError::TokenRejected) => Err("token rejected".to_string()),
        Err(e) => Err(format!(
            "backend unavailable: {}",
            match e {
                crate::backend::BackendError::Unavailable(m) => m,
                _ => "error".to_string(),
            }
        )),
    }
}

/// Common mutation body fields.
fn base_fields(args: &Value) -> Value {
    json!({
        "expected_revision": args.get("expected_revision").and_then(|v| v.as_i64()).unwrap_or(-1),
        "confirm": args.get("confirm").and_then(|v| v.as_bool()).unwrap_or(false),
        "reason": args.get("reason").and_then(|v| v.as_str()).unwrap_or(""),
    })
}

pub async fn dispatch_mutation(
    backend: &BackendClient,
    store: &DryRunStore,
    tool: &str,
    args: &Value,
) -> Result<Value, String> {
    let int_arg = |k: &str| -> Result<i64, String> {
        args.get(k)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| format!("missing integer argument '{}'", k))
    };
    let str_arg = |k: &str| -> Result<String, String> {
        args.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| format!("missing string argument '{}'", k))
    };

    if args.get("confirm").and_then(|v| v.as_bool()) != Some(true) {
        return Err("confirm=true is required for this mutation".to_string());
    }
    if args
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        return Err("a non-empty reason is required for audit".to_string());
    }
    if needs_dryrun(tool) {
        if args.get("dry_run").and_then(|v| v.as_bool()) == Some(true) {
            return dry_run(tool, args, store);
        }
        require_dryrun_op(tool, args, store)?;
    }

    let result = match tool {
        "create_service" => {
            let mut body = base_fields(args);
            for k in [
                "name",
                "port",
                "protocol",
                "is_global",
                "agent_ids",
                "chunked_transfer_encoding",
                "urldecode",
                "merge_adjacent_packets",
                "inflate_websockets",
                "decrypt_tls",
                "http_parse",
                "http2_parse",
                "alert_flag_leak",
            ] {
                if let Some(v) = args.get(k).filter(|v| !v.is_null()) {
                    body[k] = v.clone();
                }
            }
            backend.create_service(body).await
        }
        "update_service" | "set_service_agents" => {
            let mut body = base_fields(args);
            for k in ["name", "port", "protocol", "is_global", "agent_ids"] {
                if let Some(v) = args.get(k).filter(|v| !v.is_null()) {
                    body[k] = v.clone();
                }
            }
            backend.update_service(int_arg("service_id")?, body).await
        }
        "delete_service" => {
            backend
                .delete_service(int_arg("service_id")?, base_fields(args))
                .await
        }
        "set_agent_network_settings" => {
            let mut body = base_fields(args);
            for k in ["interface", "machine_ip"] {
                if let Some(v) = args.get(k).filter(|v| !v.is_null()) {
                    body[k] = v.clone();
                }
            }
            backend
                .set_agent_network_settings(&str_arg("agent_id")?, body)
                .await
        }
        "set_proxy_protection" => {
            let mut body = base_fields(args);
            body["agent_id"] = args.get("agent_id").cloned().unwrap_or(json!(""));
            body["enable"] = args.get("enable").cloned().unwrap_or(json!(false));
            if let Some(v) = args.get("tls_mitm") {
                body["tls_mitm"] = v.clone();
            }
            if let Some(v) = args.get("proxy_port").filter(|v| !v.is_null()) {
                body["proxy_port"] = v.clone();
            }
            backend
                .set_proxy_protection(int_arg("service_id")?, body)
                .await
        }
        "create_filter" => {
            let mut body = base_fields(args);
            for k in [
                "name",
                "color",
                "template_type",
                "template",
                "direction",
                "special_type",
            ] {
                if let Some(v) = args.get(k).filter(|v| !v.is_null()) {
                    body[k] = v.clone();
                }
            }
            backend.create_filter(body).await
        }
        "update_filter" => {
            let mut body = base_fields(args);
            for k in [
                "name",
                "color",
                "template_type",
                "template",
                "direction",
                "special_type",
                "enabled",
            ] {
                if let Some(v) = args.get(k).filter(|v| !v.is_null()) {
                    body[k] = v.clone();
                }
            }
            backend.update_filter(int_arg("filter_id")?, body).await
        }
        "set_filter_enabled" => {
            let mut body = base_fields(args);
            body["enabled"] = args.get("enabled").cloned().unwrap_or(json!(false));
            backend
                .set_filter_enabled(int_arg("filter_id")?, body)
                .await
        }
        "delete_filter" => {
            backend
                .delete_filter(int_arg("filter_id")?, base_fields(args))
                .await
        }
        "remove_active_block" => {
            let mut body = base_fields(args);
            body["agent_id"] = args.get("agent_id").cloned().unwrap_or(json!(""));
            body["ip"] = args.get("ip").cloned().unwrap_or(json!(""));
            backend.remove_active_block(body).await
        }
        _ => return Err(format!("unknown tool '{}'", tool)),
    };
    wrap(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_bounds_stay_in_schema_sync() {
        assert_eq!(TOOL_LIST_LIMIT_MAX, 100);
        assert!(catalog().iter().all(|t| t.schema.to_string().len() < 4096));
    }

    #[test]
    fn catalog_contains_the_exact_read_only_twelve() {
        let read: Vec<&str> = catalog()
            .iter()
            .filter(|t| t.class == ToolClass::ReadOnly)
            .map(|t| t.name)
            .collect();
        let expected = [
            "list_services",
            "list_agents",
            "search_streams",
            "get_stream_summary",
            "get_stream_messages",
            "get_packet",
            "get_message_headers",
            "get_stream_tags",
            "get_jev_verdicts",
            "list_related_streams",
            "get_soc_result",
            "get_component_health",
        ];
        assert_eq!(read, expected);
    }

    #[test]
    fn catalog_contains_the_exact_eleven_mutations() {
        let mutation: Vec<&str> = catalog()
            .iter()
            .filter(|t| t.class == ToolClass::Mutation)
            .map(|t| t.name)
            .collect();
        let expected = [
            "create_service",
            "update_service",
            "delete_service",
            "set_service_agents",
            "set_agent_network_settings",
            "set_proxy_protection",
            "create_filter",
            "update_filter",
            "delete_filter",
            "set_filter_enabled",
            "remove_active_block",
        ];
        assert_eq!(mutation, expected);
    }

    #[test]
    fn fetch_url_is_the_only_opt_in_tool_and_gates_fail_closed() {
        let fetch: Vec<&str> = catalog()
            .iter()
            .filter(|t| t.class == ToolClass::Fetch)
            .map(|t| t.name)
            .collect();
        assert_eq!(fetch, ["fetch_url"]);
        let def = catalog()
            .into_iter()
            .find(|t| t.name == "fetch_url")
            .unwrap();
        assert!(def.description.contains("MCP_ENABLE_FETCH_URL"));
        assert!(def.description.contains("disabled by default"));
        assert!(opt_in_allowed(ToolClass::Fetch, true));
        assert!(!opt_in_allowed(ToolClass::Fetch, false));
        // Every other class survives the gate regardless of the flag.
        assert!(opt_in_allowed(ToolClass::ReadOnly, false));
        assert!(opt_in_allowed(ToolClass::Mutation, false));
    }

    #[test]
    fn intended_change_is_stable_across_calls() {
        let a = json!({"service_id": 3, "expected_revision": 12, "confirm": true, "reason": "r"});
        assert_eq!(
            intended_change("delete_service", &a),
            intended_change("delete_service", &a)
        );
        assert!(intended_change("delete_service", &a).contains("service=3"));
    }

    #[test]
    fn dryrun_chain_gates_mutations() {
        let store = DryRunStore::new();
        let args = json!({
            "filter_id": 7, "enabled": false, "expected_revision": 4,
            "confirm": true, "reason": "audit me",
        });
        // Mutation without a dry-run op is rejected.
        assert!(require_dryrun_op("set_filter_enabled", &args, &store).is_err());
        let dry = dry_run("set_filter_enabled", &args, &store).unwrap();
        let op = dry["operation_id"].as_str().unwrap().to_string();
        let mut args2 = args.clone();
        args2["dry_run_op_id"] = json!(op);
        assert!(require_dryrun_op("set_filter_enabled", &args2, &store).is_ok());
        // Drifted arguments are rejected.
        let dry2 = dry_run("set_filter_enabled", &args, &store).unwrap();
        let mut args3 = args.clone();
        args3["dry_run_op_id"] = json!(dry2["operation_id"].as_str().unwrap());
        args3["enabled"] = json!(true);
        assert!(require_dryrun_op("set_filter_enabled", &args3, &store).is_err());
    }

    #[test]
    fn args_depth_and_size_are_bounded() {
        let mut deep = json!([]);
        for _ in 0..20 {
            deep = json!({"n": deep});
        }
        let deep_map = deep.as_object().unwrap().clone();
        assert!(validate_args(Some(&deep_map)).is_err());
        let ok = json!({"stream_id": 1}).as_object().unwrap().clone();
        assert!(validate_args(Some(&ok)).is_ok());
    }
}
