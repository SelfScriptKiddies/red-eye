use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Value comparison mode for header search. `Auto` keeps the legacy
/// behavior (Cookie-style substring matching); `Exact` compares the value
/// case-sensitively and is what the User-Agent drill-down uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMatchMode {
    Auto,
    Exact,
}

impl HeaderMatchMode {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None | Some("auto") => Ok(HeaderMatchMode::Auto),
            Some("exact") => Ok(HeaderMatchMode::Exact),
            Some(other) => Err(format!(
                "invalid match_mode '{}', expected \"auto\" or \"exact\"",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct HttpMessage {
    pub id: i64,
    pub stream_id: i64,
    pub packet_id: i64,
    pub is_request: bool,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status_code: Option<i16>,
    pub http_version: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i32>,
    pub body_offset: Option<i32>,
    pub body_length: Option<i32>,
    pub header_set_id: Option<i64>,
    pub timestamp: i64,
}

/// Full HTTP message with headers from header_set
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HttpMessageFull {
    #[serde(flatten)]
    pub message: HttpMessage,
    pub headers: serde_json::Value,
}

/// Search query for headers. `service_id`/`agent_id` scope the search to
/// one service and optionally one capturing agent; `match_mode` selects
/// exact (case-sensitive) value comparison over the legacy auto mode.
#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct HeaderSearchQuery {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub service_id: Option<i64>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub match_mode: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

/// Optional scope for the multi-header POST endpoint (query parameters,
/// so the body shape stays unchanged for existing callers).
#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct HeaderSearchScope {
    #[serde(default)]
    pub service_id: Option<i64>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub match_mode: Option<String>,
}

fn default_limit() -> i64 {
    100
}

/// Search result
#[derive(Debug, Serialize, ToSchema)]
pub struct HeaderSearchResult {
    pub stream_ids: Vec<i64>,
    pub total: i64,
}

/// gRPC message with decoded body
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct GrpcMessage {
    pub id: i64,
    pub stream_id: i64,
    pub http_message_id: i64,
    pub service_name: Option<String>,
    pub method_name: Option<String>,
    pub is_request: bool,
    pub body_set_id: Option<i64>,
    pub timestamp: i64,
    pub decoded_json: String,
    pub packet_id: Option<i64>,
}
