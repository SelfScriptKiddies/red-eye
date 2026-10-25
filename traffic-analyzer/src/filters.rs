pub mod bsubstr;
pub mod filterDTO;
pub mod regex;
pub mod substr;

use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::filterDTO::{Filter, FilterDirection};
use crate::filters::regex::RegexFilter;
use crate::filters::substr::SubstrFilter;
use crate::process_packet::Direction;
use log::{debug, error, warn};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;

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

// Function to add filters.
//
// Idempotent by id: filter ids come from tag_name.id (primary key), so a
// second add carrying the same id is always a stale duplicate — e.g. the
// startup DB warm-up (lib.rs::warm_filters_from_db) racing a JetStream
// redelivery of a FilterAdd the previous analyzer instance never acked.
// Appending would double-match packets (duplicate tag rows) and nothing
// would heal it later: sync_filters diffs by id SET (duplicate ids
// collapse, so neither branch fires) and edit_filter removes only the
// FIRST entry with the id. So we REPLACE an existing entry with the same
// id instead of appending a second one.
/// Returns true when the filter was registered, false when the row was
/// rejected (unsupported type / bad payload — reason is logged). Callers
/// that only need the side effect may ignore the result.
pub fn add_filter(
    id: usize,
    pattern_name: String,
    pattern_type: String,
    payload: &[u8],
    direction: FilterDirection,
) -> bool {
    let mut filters = PACKET_FILTERS.write().unwrap();

    // Build first: on a bad filter (unsupported type / bad regex payload)
    // we return without touching an already-registered entry with this id.
    //
    // bsubstr templates are HEX strings (the UI's quick-block and the tag
    // form both send hex, e.g. "deadbeef"); the filter must search for the
    // DECODED bytes, not the literal ASCII of the hex string — same
    // convention as the agent's harpoon::parse_filter. Payloads that are
    // not valid hex fall back to raw bytes (backwards compatibility).
    let filter: Box<dyn Filter> = match pattern_type.to_lowercase().as_str() {
        "regex" => {
            let payload_str = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Error converting payload to string: {}", e);
                    return false;
                }
            };

            match RegexFilter::new(id, &pattern_name, &payload_str) {
                Ok(filter) => Box::new(filter),
                Err(e) => {
                    error!("Error creating regex filter: {}", e);
                    return false;
                }
            }
        }
        "bsubstr" => {
            let pattern_bytes = match hex::decode(payload) {
                Ok(bytes) => bytes,
                Err(_) => {
                    warn!(
                        "bsubstr filter '{}' payload is not valid hex, using raw bytes",
                        pattern_name
                    );
                    payload.to_vec()
                }
            };
            Box::new(BinarySubstrFilter::new(id, &pattern_name, &pattern_bytes))
        }
        "substr" => Box::new(SubstrFilter::new(id, &pattern_name, payload)),
        _ => {
            error!("Unsupported filter type: {}", pattern_type);
            return false;
        }
    };

    // Drop any stale entry with the same id, then insert the fresh one.
    filters.retain(|entry| entry.filter.get_id() != id);
    filters.push(FilterEntry { filter, direction });
    true
}

pub fn remove_filter(index: usize) {
    let mut filters = PACKET_FILTERS.write().unwrap();

    // Find the index of the filter with the specified id
    if let Some(position) = filters
        .iter()
        .position(|entry| entry.filter.get_id() == index)
    {
        // Remove the filter by found index
        filters.remove(position);
        debug!("Filter with id {} removed successfully", index);
    } else {
        error!("Filter with id {} not found", index);
    }
}

pub fn edit_filter(
    index: usize,
    pattern_name: String,
    pattern_type: String,
    payload: &[u8],
    direction: FilterDirection,
) {
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
    filters
        .iter()
        .map(|entry| (entry.filter.get_id(), entry.filter.get_pattern_name()))
        .collect()
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
    let to_remove: Vec<_> = existing_ids.difference(&incoming_ids).cloned().collect();

    if missing.is_empty() && to_remove.is_empty() {
        debug!("Filters sync: no changes needed");
        return;
    }

    debug!(
        "Filters sync: adding {} missing, removing {} stale",
        missing.len(),
        to_remove.len()
    );

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
pub fn apply_filters(
    packet: &[u8],
    direction: &Direction,
) -> Vec<(usize, Vec<filterDTO::FilterTrigger>)> {
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

    /// PACKET_FILTERS is a process-global: tests that mutate it must not run
    /// concurrently — sync_filters REMOVES every id absent from its synced
    /// set, so a parallel test's freshly added filter vanishes mid-assert.
    static FILTERS_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn inbound_filter_matches_only_inbound_packets() {
        assert!(direction_matches(
            &FilterDirection::Inbound,
            &Direction::Inbound
        ));
        assert!(!direction_matches(
            &FilterDirection::Inbound,
            &Direction::Outbound
        ));
    }

    #[test]
    fn outbound_filter_matches_only_outbound_packets() {
        assert!(!direction_matches(
            &FilterDirection::Outbound,
            &Direction::Inbound
        ));
        assert!(direction_matches(
            &FilterDirection::Outbound,
            &Direction::Outbound
        ));
    }

    #[test]
    fn both_direction_filter_matches_everything() {
        assert!(direction_matches(
            &FilterDirection::Both,
            &Direction::Inbound
        ));
        assert!(direction_matches(
            &FilterDirection::Both,
            &Direction::Outbound
        ));
    }

    #[test]
    fn apply_filters_respects_filter_direction() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // Unique high id so we cannot collide with filters added by live
        // settings updates between our add and remove.
        let id = 987_654;
        add_filter(
            id,
            "dir_test".to_string(),
            "substr".to_string(),
            b"flag",
            FilterDirection::Inbound,
        );

        let payload = b"flag{xxx}";

        // Inbound packet -> trigger fires
        let hit = apply_filters(payload, &Direction::Inbound);
        assert!(
            hit.iter().any(|(fid, t)| *fid == id && !t.is_empty()),
            "inbound filter must trigger on inbound packet"
        );

        // Outbound packet -> no trigger from this filter
        let miss = apply_filters(payload, &Direction::Outbound);
        assert!(
            !miss.iter().any(|(fid, _)| *fid == id),
            "inbound filter must NOT trigger on outbound packet"
        );

        // Both -> fires in either direction
        edit_filter(
            id,
            "dir_test".to_string(),
            "substr".to_string(),
            b"flag",
            FilterDirection::Both,
        );
        assert!(apply_filters(payload, &Direction::Inbound)
            .iter()
            .any(|(fid, t)| *fid == id && !t.is_empty()));
        assert!(apply_filters(payload, &Direction::Outbound)
            .iter()
            .any(|(fid, t)| *fid == id && !t.is_empty()));

        remove_filter(id);
    }

    /// Startup DB warm-up racing a redelivered FilterAdd (a message the
    /// previous analyzer instance never acked): the same id must be
    /// registered exactly once, otherwise every matching packet would
    /// double-match (duplicate tag rows downstream).
    #[test]
    fn add_filter_is_idempotent_by_id() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_655;
        add_filter(
            id,
            "dup".to_string(),
            "substr".to_string(),
            b"flag",
            FilterDirection::Both,
        );
        add_filter(
            id,
            "dup".to_string(),
            "substr".to_string(),
            b"flag",
            FilterDirection::Both,
        );
        assert_eq!(
            get_filter_ids().iter().filter(|&&fid| fid == id).count(),
            1,
            "double add for the same id must replace, not duplicate"
        );
        remove_filter(id);
    }

    /// The first ConfigSync after startup carries the same rows the DB
    /// warm-up just loaded. sync_filters diffs by id set (verified in the
    /// ConfigSync handler, configuration/updater.rs): ids already present
    /// are neither re-added nor removed, so the sync is a no-op and the
    /// warmed set stays deduplicated.
    #[test]
    fn sync_filters_right_after_warmup_does_not_duplicate() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_656;
        let rows = vec![(
            id,
            "warm".to_string(),
            "substr".to_string(),
            b"warm".to_vec(),
            FilterDirection::Inbound,
        )];

        // Startup warm-up path: plain add_filter per row.
        for (fid, name, ftype, payload, direction) in rows.clone() {
            add_filter(fid, name, ftype, &payload, direction);
        }

        // First ConfigSync arrives with the same rows.
        sync_filters(rows);

        assert_eq!(
            get_filter_ids().iter().filter(|&&fid| fid == id).count(),
            1,
            "ConfigSync right after DB warm-up must not duplicate filters"
        );
        remove_filter(id);
    }

    /// bsubstr templates arrive as HEX strings on the wire (backend sends
    /// `tag_name.template.as_bytes()`), and the DB warm-up loads them the
    /// same way. The filter must decode hex and search for the BYTES —
    /// matching the literal ASCII "deadbeef" would never find binary
    /// content. Mirrors agent harpoon::parse_filter semantics.
    #[test]
    fn bsubstr_hex_template_matches_decoded_bytes() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_660;

        assert!(add_filter(
            id,
            "bin".to_string(),
            "bsubstr".to_string(),
            b"deadbeef", // hex STRING — decodes to [0xde, 0xad, 0xbe, 0xef]
            FilterDirection::Both,
        ));

        // Real binary content matches...
        let hit = apply_filters(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0x11], &Direction::Inbound);
        assert!(
            hit.iter().any(|(fid, t)| *fid == id && !t.is_empty()),
            "bsubstr must match the decoded bytes, not the hex text"
        );

        // ...the literal ASCII of the hex string must NOT match.
        let miss = apply_filters(b"xxdeadbeefyy", &Direction::Inbound);
        assert!(!miss.iter().any(|(fid, _)| *fid == id));

        remove_filter(id);
    }

    /// Backwards compatibility: a bsubstr payload that is not valid hex
    /// (legacy plain-text rows) still searches for its raw bytes.
    #[test]
    fn bsubstr_non_hex_payload_falls_back_to_raw_bytes() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_661;

        assert!(add_filter(
            id,
            "legacy".to_string(),
            "bsubstr".to_string(),
            b"zz not hex zz",
            FilterDirection::Both,
        ));

        let hit = apply_filters(b"prefix zz not hex zz suffix", &Direction::Outbound);
        assert!(hit.iter().any(|(fid, t)| *fid == id && !t.is_empty()));

        remove_filter(id);
    }

    /// If the DB changed between warm-up and the first sync (row deleted),
    /// sync_filters removes the stale id and keeps the live one exactly
    /// once. Note: sync_filters is a diff, not a replace — it never
    /// re-adds ids that already exist (payload changes travel as
    /// FilterUpdate/edit_filter, covered by the next test).
    #[test]
    fn sync_filters_converges_after_db_change() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let stale_id = 987_657;
        let live_id = 987_658;

        add_filter(
            stale_id,
            "stale".to_string(),
            "substr".to_string(),
            b"stale",
            FilterDirection::Both,
        );
        add_filter(
            live_id,
            "live".to_string(),
            "substr".to_string(),
            b"live",
            FilterDirection::Both,
        );

        // Sync that no longer contains stale_id.
        sync_filters(vec![(
            live_id,
            "live".to_string(),
            "substr".to_string(),
            b"live".to_vec(),
            FilterDirection::Both,
        )]);

        let ids = get_filter_ids();
        assert!(
            !ids.contains(&stale_id),
            "stale filter must be removed by sync"
        );
        assert_eq!(ids.iter().filter(|&&fid| fid == live_id).count(), 1);
        remove_filter(live_id);
    }

    /// A redelivered FilterUpdate racing the warm-up goes through
    /// edit_filter = remove + add; combined with add_filter's
    /// replace-by-id this converges to exactly one entry with the NEW
    /// payload/direction.
    #[test]
    fn edit_filter_after_warmup_replaces_not_duplicates() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_659;
        add_filter(
            id,
            "warm".to_string(),
            "substr".to_string(),
            b"old",
            FilterDirection::Both,
        );
        edit_filter(
            id,
            "warm".to_string(),
            "substr".to_string(),
            b"new",
            FilterDirection::Inbound,
        );

        assert_eq!(
            get_filter_ids().iter().filter(|&&fid| fid == id).count(),
            1,
            "edit after warm-up must leave exactly one entry"
        );
        // New payload + direction took effect.
        assert!(apply_filters(b"new", &Direction::Inbound)
            .iter()
            .any(|(fid, t)| *fid == id && !t.is_empty()));
        assert!(!apply_filters(b"new", &Direction::Outbound)
            .iter()
            .any(|(fid, _)| *fid == id));
        assert!(!apply_filters(b"old", &Direction::Inbound)
            .iter()
            .any(|(fid, _)| *fid == id));
        remove_filter(id);
    }
}
