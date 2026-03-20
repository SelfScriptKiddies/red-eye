use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct HttpMessage {
    pub id: i32,
    pub stream_id: i32,
    pub packet_id: i32,
    pub is_request: bool,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status_code: Option<i16>,
    pub http_version: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i32>,
    pub body_offset: Option<i32>,
    pub body_length: Option<i32>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct HttpHeader {
    pub id: i32,
    pub message_id: i32,
    pub name: String,
    pub value: String,
}

/// Full HTTP message with all headers
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HttpMessageFull {
    #[serde(flatten)]
    pub message: HttpMessage,
    pub headers: Vec<HttpHeader>,
}

/// Search query for headers
#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct HeaderSearchQuery {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    100
}

/// Search result — stream IDs matching header criteria
#[derive(Debug, Serialize, ToSchema)]
pub struct HeaderSearchResult {
    pub stream_ids: Vec<i32>,
    pub total: i64,
}
