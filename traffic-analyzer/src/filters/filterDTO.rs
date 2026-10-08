use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};

/// Direction a filter is allowed to match in (wire + storage representation).
///
/// Mapping (derived from code, see `direction_matches` in filters.rs):
/// - `Inbound`  — traffic arriving at the analyzed machine (packets.direction = 'dest')
/// - `Outbound` — traffic sent by the analyzed machine    (packets.direction = 'orig')
/// - `Both`     — either direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum FilterDirection {
    Inbound,
    Outbound,
    Both
}

impl FilterDirection {
    /// Map a tag_name.direction column value to a FilterDirection.
    ///
    /// Mirrors the backend dto mapping (backend/src/api/updater.rs,
    /// `impl From<tag_dto::TagName> for Filter`): 'in' -> Inbound,
    /// 'out' -> Outbound, anything else (incl. 'both' and garbage) -> Both.
    /// The NATS path gets this mapping for free because the backend performs
    /// it before encoding; the DB warm-up path (lib.rs) must do it locally,
    /// so it lives here to keep a single implementation.
    pub fn from_tag_direction(direction: &str) -> Self {
        match direction.to_lowercase().as_str() {
            "in" => FilterDirection::Inbound,
            "out" => FilterDirection::Outbound,
            _ => FilterDirection::Both,
        }
    }
}

pub trait Filter: Send + Sync {
    fn get_id(&self) -> usize;
    fn get_pattern_name(&self) -> String;
    fn processed(&self, packet: &[u8]) -> Vec<FilterTrigger>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterDTO {
    pub id: usize,
    pub pattern_name: String,
    pub filter_type: String,
    pub filter_payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterTrigger {
    pub start_index: usize,
    pub end_index: usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DB warm-up must map tag_name.direction exactly like the backend
    /// does before encoding a dto::Filter onto the NATS settings topic.
    #[test]
    fn from_tag_direction_mirrors_backend_dto_mapping() {
        assert_eq!(FilterDirection::from_tag_direction("in"), FilterDirection::Inbound);
        assert_eq!(FilterDirection::from_tag_direction("out"), FilterDirection::Outbound);
        assert_eq!(FilterDirection::from_tag_direction("both"), FilterDirection::Both);
        // Backend lowercases before matching; unknown values fall back to Both.
        assert_eq!(FilterDirection::from_tag_direction("IN"), FilterDirection::Inbound);
        assert_eq!(FilterDirection::from_tag_direction("Out"), FilterDirection::Outbound);
        assert_eq!(FilterDirection::from_tag_direction("BOTH"), FilterDirection::Both);
        assert_eq!(FilterDirection::from_tag_direction(""), FilterDirection::Both);
        assert_eq!(FilterDirection::from_tag_direction("garbage"), FilterDirection::Both);
    }
}
