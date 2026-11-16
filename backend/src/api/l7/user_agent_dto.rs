use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// One listed User-Agent value. The overflow entry carries no value and is
/// never clickable: its stored marker never leaves the database.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UserAgentItem {
    pub user_agent: Option<String>,
    pub message_count: i64,
    pub first_seen: i64,
    pub last_seen: i64,
    pub agent_ids: Vec<String>,
    pub is_overflow: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserAgentsResponse {
    pub items: Vec<UserAgentItem>,
    pub next_cursor: Option<String>,
    pub has_overflow: bool,
    /// Unix seconds of the first successful aggregate write. None means
    /// the analyzer has not aggregated anything yet: the listing covers
    /// only traffic after that moment.
    pub aggregation_started: Option<i64>,
}

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct UserAgentsQuery {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub order: Option<String>,
}

pub const UA_LIMIT_DEFAULT: i64 = 100;
pub const UA_LIMIT_MAX: i64 = 200;

fn default_limit() -> i64 {
    UA_LIMIT_DEFAULT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UaSort {
    Count,
    LastSeen,
}

impl UaSort {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None | Some("last_seen") => Ok(UaSort::LastSeen),
            Some("count") => Ok(UaSort::Count),
            Some(other) => Err(format!(
                "invalid sort '{}', expected \"count\" or \"last_seen\"",
                other
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            UaSort::Count => "count",
            UaSort::LastSeen => "last_seen",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UaOrder {
    Desc,
    Asc,
}

impl UaOrder {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None | Some("desc") => Ok(UaOrder::Desc),
            Some("asc") => Ok(UaOrder::Asc),
            Some(other) => Err(format!(
                "invalid order '{}', expected \"desc\" or \"asc\"",
                other
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            UaOrder::Desc => "desc",
            UaOrder::Asc => "asc",
        }
    }
}

/// Sort key of the last row of a page, carried inside the cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UaCursorKey {
    pub message_count: i64,
    pub last_seen: i64,
    pub user_agent: String,
}

/// Cursor payload. The fingerprint fields (service, agent, sort, order)
/// make a cursor from one listing view unusable in another: the server
/// rejects the mismatch instead of silently re-paginating different data.
#[derive(Debug, Serialize, Deserialize)]
struct UaCursorPayload {
    v: u8,
    service_id: i64,
    agent_id: Option<String>,
    sort: String,
    order: String,
    #[serde(flatten)]
    key: UaCursorKey,
}

const CURSOR_VERSION: u8 = 1;

pub fn encode_cursor(
    service_id: i64,
    agent_id: Option<&str>,
    sort: UaSort,
    order: UaOrder,
    key: &UaCursorKey,
) -> String {
    let payload = UaCursorPayload {
        v: CURSOR_VERSION,
        service_id,
        agent_id: agent_id.map(str::to_string),
        sort: sort.as_str().to_string(),
        order: order.as_str().to_string(),
        key: key.clone(),
    };
    let json = serde_json::to_vec(&payload).unwrap_or_default();
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn decode_cursor(
    raw: &str,
    service_id: i64,
    agent_id: Option<&str>,
    sort: UaSort,
    order: UaOrder,
) -> Result<UaCursorKey, String> {
    use base64::Engine as _;
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw.trim())
        .map_err(|_| "invalid cursor encoding".to_string())?;
    let payload: UaCursorPayload =
        serde_json::from_slice(&json).map_err(|_| "invalid cursor payload".to_string())?;
    if payload.v != CURSOR_VERSION {
        return Err("unsupported cursor version".to_string());
    }
    let fingerprint_ok = payload.service_id == service_id
        && payload.agent_id.as_deref() == agent_id
        && payload.sort == sort.as_str()
        && payload.order == order.as_str();
    if !fingerprint_ok {
        return Err(
            "cursor belongs to a different service, agent filter, sort or order".to_string(),
        );
    }
    Ok(payload.key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(count: i64, seen: i64, ua: &str) -> UaCursorKey {
        UaCursorKey {
            message_count: count,
            last_seen: seen,
            user_agent: ua.to_string(),
        }
    }

    #[test]
    fn cursor_roundtrips_and_carries_the_key() {
        let encoded = encode_cursor(
            7,
            Some("agent-1"),
            UaSort::Count,
            UaOrder::Desc,
            &key(42, 1_000, "curl/8.0"),
        );
        let decoded = decode_cursor(&encoded, 7, Some("agent-1"), UaSort::Count, UaOrder::Desc)
            .expect("cursor valid");
        assert_eq!(decoded.message_count, 42);
        assert_eq!(decoded.last_seen, 1_000);
        assert_eq!(decoded.user_agent, "curl/8.0");
    }

    #[test]
    fn cursor_rejects_mismatched_filters_or_sort() {
        let encoded = encode_cursor(7, None, UaSort::Count, UaOrder::Desc, &key(1, 1, "x"));
        assert!(decode_cursor(&encoded, 8, None, UaSort::Count, UaOrder::Desc).is_err());
        assert!(decode_cursor(&encoded, 7, Some("a"), UaSort::Count, UaOrder::Desc).is_err());
        assert!(decode_cursor(&encoded, 7, None, UaSort::LastSeen, UaOrder::Desc).is_err());
        assert!(decode_cursor(&encoded, 7, None, UaSort::Count, UaOrder::Asc).is_err());
    }

    #[test]
    fn garbage_cursor_is_an_error_not_a_fallback() {
        assert!(decode_cursor("!!!", 1, None, UaSort::Count, UaOrder::Desc).is_err());
        assert!(decode_cursor("", 1, None, UaSort::Count, UaOrder::Desc).is_err());
    }

    #[test]
    fn sort_and_order_parse_strictly() {
        assert_eq!(UaSort::parse(None), Ok(UaSort::LastSeen));
        assert_eq!(
            UaSort::parse(Some("COUNT")),
            Err("invalid sort 'COUNT', expected \"count\" or \"last_seen\"".to_string())
        );
        assert_eq!(UaOrder::parse(None), Ok(UaOrder::Desc));
        assert!(UaOrder::parse(Some("descending")).is_err());
    }
}
