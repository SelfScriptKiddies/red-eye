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
