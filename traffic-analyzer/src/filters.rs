pub mod regex;
pub mod bsubstr;
pub mod substr;
pub mod filterDTO;

use log::{error, debug};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;
use crate::filters::filterDTO::{Filter, FilterDirection};
use crate::filters::regex::RegexFilter;
use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::substr::SubstrFilter;
use crate::process_packet::Direction;

/// A registered filter plus the direction it is allowed to match in.
/// Direction comes from tag_name ('in'/'out'/'both') via the settings
/// updates topic; without it a checker's own submitted flag matches the
/// "stolen flag" filter and gets submitted to the farm.
struct FilterEntry {
    filter: Box<dyn Filter>,
    direction: FilterDirection,
}

// Global storage for filters
static PACKET_FILTERS: Lazy<RwLock<Vec<FilterEntry>>> = Lazy::new(|| {
    RwLock::new(vec![
        // FilterEntry { filter: Box::new(RegexFilter::new(1, "regex_filter", r"GET /\w*").unwrap()), direction: FilterDirection::Both },
        // FilterEntry { filter: Box::new(BinarySubstrFilter::new(2, "binary_substr_filter", b"1234567890")), direction: FilterDirection::Both },
        // FilterEntry { filter: Box::new(SubstrFilter::new(3, "substr_filter", b"1234567890")), direction: FilterDirection::Both },
    ])
});

/// Pure direction check: does a filter with this direction apply to a packet
/// with this packet direction?
///
/// Semantics (derived from code, not guessed):
/// - `process_packet::find_service_and_direction`: the analyzed machine as
///   packet DESTINATION -> `Direction::Inbound`; as SOURCE -> `Direction::Outbound`.
/// - `db.rs insert_packet`/`insert_packets_batch`: `Inbound -> 'dest'`, `Outbound -> 'orig'`.
/// - backend `tag_repository.rs` counts `p.direction='dest'` as incoming,
///   `'orig'` as outgoing; backend `updater.rs` maps tag_name 'in' ->
///   `FilterDirection::Inbound`. Hence Inbound==dest, Outbound==orig.
pub fn direction_matches(filter_direction: &FilterDirection, packet_direction: &Direction) -> bool {
    match filter_direction {
        FilterDirection::Both => true,
        FilterDirection::Inbound => matches!(packet_direction, Direction::Inbound),
        FilterDirection::Outbound => matches!(packet_direction, Direction::Outbound),
    }
}

// Function to add filters
pub fn add_filter(id: usize, pattern_name: String, pattern_type: String, payload: &[u8], direction: FilterDirection) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    match pattern_type.to_lowercase().as_str() {
        "regex" => {
            let payload_str = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Error converting payload to string: {}", e);
                    return;
                }
            };

            match RegexFilter::new(id, &pattern_name, &payload_str) {
                Ok(filter) => filters.push(FilterEntry { filter: Box::new(filter), direction }),
                Err(e) => error!("Error creating regex filter: {}", e),
            }
        }
        "bsubstr" => {
            filters.push(FilterEntry { filter: Box::new(BinarySubstrFilter::new(id, &pattern_name, &payload)), direction });
        }
        "substr" => {
            filters.push(FilterEntry { filter: Box::new(SubstrFilter::new(id, &pattern_name, &payload)), direction });
        }
        _ => {
            error!("Unsupported filter type: {}", pattern_type);
        }
    }
}

pub fn remove_filter(index: usize) {
    let mut filters = PACKET_FILTERS.write().unwrap();

    // Find the index of the filter with the specified id
    if let Some(position) = filters.iter().position(|entry| entry.filter.get_id() == index) {
        // Remove the filter by found index
        filters.remove(position);
        debug!("Filter with id {} removed successfully", index);
    } else {
        error!("Filter with id {} not found", index);
    }

}

pub fn edit_filter(index: usize, pattern_name: String, pattern_type: String, payload: &[u8], direction: FilterDirection) {
    remove_filter(index);
    add_filter(index, pattern_name, pattern_type, payload, direction);
}

pub fn get_filter_ids() -> Vec<usize> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters.iter().map(|entry| entry.filter.get_id()).collect()
}

/// Snapshot of filter_id -> tag name (tag_name.name in the DB). Used by the
/// batch flusher to maintain stream_tag with the SAME strings the old
/// materialized view produced (tag rows store tag_name.id; the view joined
/// them back to names), so UI semantics don't change.
pub fn filter_names_snapshot() -> HashMap<usize, String> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters.iter().map(|entry| (entry.filter.get_id(), entry.filter.get_pattern_name())).collect()
}

pub fn sync_filters(filters_data: Vec<(usize, String, String, Vec<u8>, FilterDirection)>) {
    use std::collections::HashSet;

    let incoming_ids: HashSet<usize> = filters_data.iter().map(|(id, _, _, _, _)| *id).collect();
    let existing_ids: HashSet<usize> = get_filter_ids().into_iter().collect();

    // Находим отсутствующие и лишние
    let missing: Vec<_> = filters_data
        .into_iter()
        .filter(|(id, _, _, _, _)| !existing_ids.contains(id))
        .collect();
    let to_remove: Vec<_> = existing_ids
        .difference(&incoming_ids)
        .cloned()
        .collect();

    if missing.is_empty() && to_remove.is_empty() {
        debug!("Filters sync: no changes needed");
        return;
    }

    debug!("Filters sync: adding {} missing, removing {} stale", missing.len(), to_remove.len());

    // Удаляем лишние
    for id in to_remove {
        remove_filter(id);
    }

    // Добавляем отсутствующие
    for (id, pattern_name, pattern_type, payload, direction) in missing {
        add_filter(id, pattern_name, pattern_type, &payload, direction);
    }
}

// Function to apply all filters to a packet, honoring each filter's direction
pub fn apply_filters(packet: &[u8], direction: &Direction) -> Vec<(usize, Vec<filterDTO::FilterTrigger>)> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut results: Vec<(usize, Vec<filterDTO::FilterTrigger>)> = Vec::new();

    for entry in filters.iter() {
        if !direction_matches(&entry.direction, direction) {
            continue;
        }
        let triggers = entry.filter.processed(packet);
        if !triggers.is_empty() {
            results.push((entry.filter.get_id(), triggers));
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_filter_matches_only_inbound_packets() {
        assert!(direction_matches(&FilterDirection::Inbound, &Direction::Inbound));
        assert!(!direction_matches(&FilterDirection::Inbound, &Direction::Outbound));
    }

    #[test]
    fn outbound_filter_matches_only_outbound_packets() {
        assert!(!direction_matches(&FilterDirection::Outbound, &Direction::Inbound));
        assert!(direction_matches(&FilterDirection::Outbound, &Direction::Outbound));
    }

    #[test]
    fn both_direction_filter_matches_everything() {
        assert!(direction_matches(&FilterDirection::Both, &Direction::Inbound));
        assert!(direction_matches(&FilterDirection::Both, &Direction::Outbound));
    }

    #[test]
    fn apply_filters_respects_filter_direction() {
        // Unique high id so we cannot collide with filters added by live
        // settings updates between our add and remove.
        let id = 987_654;
        add_filter(id, "dir_test".to_string(), "substr".to_string(), b"flag", FilterDirection::Inbound);

        let payload = b"flag{xxx}";

        // Inbound packet -> trigger fires
        let hit = apply_filters(payload, &Direction::Inbound);
        assert!(hit.iter().any(|(fid, t)| *fid == id && !t.is_empty()),
            "inbound filter must trigger on inbound packet");

        // Outbound packet -> no trigger from this filter
        let miss = apply_filters(payload, &Direction::Outbound);
        assert!(!miss.iter().any(|(fid, _)| *fid == id),
            "inbound filter must NOT trigger on outbound packet");

        // Both -> fires in either direction
        edit_filter(id, "dir_test".to_string(), "substr".to_string(), b"flag", FilterDirection::Both);
        assert!(apply_filters(payload, &Direction::Inbound).iter().any(|(fid, t)| *fid == id && !t.is_empty()));
        assert!(apply_filters(payload, &Direction::Outbound).iter().any(|(fid, t)| *fid == id && !t.is_empty()));

        remove_filter(id);
    }
}
