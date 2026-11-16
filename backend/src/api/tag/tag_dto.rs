use serde::{Deserialize, Serialize};

/// Matcher kinds of the unified filter model. template_type keeps its
/// historical column name; regex/substr/bsubstr preserve their old
/// behavior, ip_cidr and expression are new variants.
pub const MATCHER_TYPES: [&str; 5] = ["regex", "substr", "bsubstr", "ip_cidr", "expression"];

/// Actions an enabled and matching filter can produce.
pub const ACTIONS: [&str; 5] = ["tag", "flag", "alert", "block", "block_connection"];

/// Filter direction values (tag_name.direction).
pub const DIRECTIONS: [&str; 3] = ["in", "out", "both"];

/// Threshold counter grouping values.
pub const GROUP_BYS: [&str; 3] = ["remote_ip", "stream", "service"];

/// IP/CIDR endpoint values (original packet, before tuple normalization).
pub const IP_ENDPOINTS: [&str; 2] = ["src", "dst"];

/// Pattern length limit shared by every payload matcher (regex source,
/// substring bytes, bsubstr hex input).
pub const MAX_PATTERN_LEN: usize = 255;

/// Expression source length limit. Kept in sync with the filter-dsl crate
/// limit (which enforces it at compile time as well).
pub const MAX_EXPRESSION_SOURCE_LEN: usize = 4096;

#[derive(Deserialize, Serialize, utoipa::ToSchema, Clone)]
pub struct TagName {
    pub id: i64,
    pub name: String,
    pub color: i16,
    /// Matcher kind: regex | substr | bsubstr | ip_cidr | expression
    pub template_type: String,
    /// Payload pattern. bsubstr carries validated hex; ip_cidr leaves it
    /// empty (the CIDR lives in ip_network).
    pub template: String,
    /// in | out | both, relative to the protected machine
    pub direction: String,
    /// Legacy special type (pre-unified rows only; no longer written).
    pub special_type: Option<String>,
    /// tag | flag | alert | block | block_connection
    pub action: String,
    pub enabled: bool,
    /// Normalized CIDR network for ip_cidr matchers (single addresses are
    /// stored as their host prefix).
    pub ip_network: Option<String>,
    /// src | dst for ip_cidr matchers.
    pub ip_endpoint: Option<String>,
    /// Validated CEL-style expression source for expression matchers.
    pub expression_source: Option<String>,
    /// TRUE = every current and future service; FALSE = service_ids only.
    pub service_targets_global: bool,
    /// TRUE = every current and future agent; FALSE = agent_ids only.
    pub agent_targets_global: bool,
    pub service_ids: Vec<i64>,
    pub agent_ids: Vec<String>,
    /// Threshold configuration; valid only for alert and block actions.
    pub threshold_count: Option<i32>,
    pub threshold_window_secs: Option<i32>,
    pub threshold_group_by: Option<String>,
    pub threshold_cooldown_secs: Option<i32>,
    pub threshold_block_secs: Option<i32>,
    /// Expiry: an expired filter stops matching and its active blocks are
    /// removed.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Control-plane revision the filter was last mutated at.
    pub revision: i64,
}

/// Create/update input. `special_type` is legacy input kept for older
/// clients: when `action` is absent it is mapped onto the action.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct TagNameCreate {
    pub name: String,
    pub color: i16,
    pub template_type: String,
    pub template: String,
    #[serde(default = "default_direction")]
    pub direction: String,
    #[serde(default)]
    pub special_type: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub ip_network: Option<String>,
    #[serde(default)]
    pub ip_endpoint: Option<String>,
    #[serde(default)]
    pub expression_source: Option<String>,
    #[serde(default = "default_true")]
    pub service_targets_global: bool,
    #[serde(default = "default_true")]
    pub agent_targets_global: bool,
    #[serde(default)]
    pub service_ids: Vec<i64>,
    #[serde(default)]
    pub agent_ids: Vec<String>,
    #[serde(default)]
    pub threshold_count: Option<i32>,
    #[serde(default)]
    pub threshold_window_secs: Option<i32>,
    #[serde(default)]
    pub threshold_group_by: Option<String>,
    #[serde(default)]
    pub threshold_cooldown_secs: Option<i32>,
    #[serde(default)]
    pub threshold_block_secs: Option<i32>,
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn default_direction() -> String {
    "both".to_string()
}

fn default_true() -> bool {
    true
}

impl TagNameCreate {
    /// Resolve the action: explicit `action` wins; otherwise the legacy
    /// `special_type` maps without changing its product meaning.
    pub fn resolved_action(&self) -> String {
        if let Some(action) = &self.action {
            return action.clone();
        }
        match self.special_type.as_deref() {
            Some("flag") => "flag".to_string(),
            Some("alert") => "alert".to_string(),
            Some("block") => "block".to_string(),
            Some("block_conn") => "block_connection".to_string(),
            _ => "tag".to_string(),
        }
    }
}

/// Triggered tag, so filters found something
#[derive(sqlx::FromRow, Deserialize, Serialize, utoipa::ToSchema)]
pub struct Tag {
    pub id: i64,
    pub name: String,
    pub color: i16,
    pub packet_id: i64,
    pub tag_id: i64,
    pub start_index: i32,
    pub end_index: i32,
}

/// Filter validation error: rejected before persistence, mapped to 400 by
/// the API layer.
#[derive(Debug)]
pub struct TagValidationError(pub String);

impl std::fmt::Display for TagValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
