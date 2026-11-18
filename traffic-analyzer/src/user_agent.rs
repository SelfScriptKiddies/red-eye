use std::collections::{HashMap, HashSet};

/// Upper bound on one stored exact User-Agent value (UTF-8 bytes). Longer
/// values are never truncated into a plausible exact value; they count
/// only in the bucket's overflow row.
pub const MAX_VALUE_BYTES: usize = 1024;

/// Ordinary distinct values per (service, agent, bucket). Beyond this
/// bound new distinct values contribute to the overflow row; existing
/// ordinary rows keep updating.
pub const MAX_DISTINCT_PER_BUCKET: i64 = 1000;

/// Bucket width in seconds (five-minute buckets, matching the partition
/// interval the rest of the analyzer data uses).
pub const BUCKET_SECS: i64 = 300;

/// Reserved internal value stored in overflow rows. Never returned by any
/// read path: the API reports overflow rows only as is_overflow=true
/// without their value. Starts with LF, which the classifier below never
/// admits as an ordinary value, so it cannot collide with a real header.
pub const OVERFLOW_MARKER: &str = "\noverflow";

/// Classified User-Agent of one HTTP request message.
#[derive(Debug, Clone, PartialEq)]
pub enum UserAgentStat {
    /// No contribution (response message, header absent, or blank value).
    None,
    /// Exact trimmed header value: preserved byte-for-byte, never
    /// lowercased, normalized or truncated.
    Exact(String),
    /// Counts only in the bucket's overflow row.
    Overflow(UaOverflowReason),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UaOverflowReason {
    /// Value contains NUL, CR or LF and cannot round-trip as an exact row.
    ControlChars,
    /// Value exceeds MAX_VALUE_BYTES UTF-8 bytes.
    Oversized,
}

/// ASCII case-insensitive User-Agent extraction from request headers.
/// Leading/trailing HTTP optional whitespace (SP, HTAB) is trimmed.
pub fn classify(headers: &[(String, String)], is_request: bool) -> UserAgentStat {
    if !is_request {
        return UserAgentStat::None;
    }
    let Some((_, raw)) = headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("user-agent"))
    else {
        return UserAgentStat::None;
    };
    let trimmed = raw.trim_matches([' ', '\t']);
    if trimmed.is_empty() {
        return UserAgentStat::None;
    }
    if trimmed.bytes().any(|b| matches!(b, 0x00 | 0x0a | 0x0d)) {
        return UserAgentStat::Overflow(UaOverflowReason::ControlChars);
    }
    if trimmed.len() > MAX_VALUE_BYTES {
        return UserAgentStat::Overflow(UaOverflowReason::Oversized);
    }
    UserAgentStat::Exact(trimmed.to_string())
}

/// Bucket start for a capture timestamp (epoch seconds).
pub fn bucket_start(timestamp: i64) -> i64 {
    (timestamp / BUCKET_SECS) * BUCKET_SECS
}

/// Aggregated key: (service_id, agent_id, bucket_start, user_agent,
/// is_overflow). Exactly the aggregate table's PK column order.
pub type UaGroupKey = (i64, String, i64, String, bool);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UaGroupAggregate {
    pub message_count: i64,
    pub first_seen: i64,
    pub last_seen: i64,
}

/// Group one flush batch's User-Agent contributions into aggregate upsert
/// rows. Duplicate values inside the batch collapse into one increment, so
/// a 500-message batch of the same UA writes one row, not 500.
pub fn group_contributions(
    items: impl IntoIterator<Item = (i64, String, i64, UserAgentStat)>,
) -> HashMap<UaGroupKey, UaGroupAggregate> {
    let mut groups: HashMap<UaGroupKey, UaGroupAggregate> = HashMap::new();
    for (service_id, agent_id, timestamp, stat) in items {
        let key = match stat {
            UserAgentStat::None => continue,
            UserAgentStat::Exact(value) => {
                (service_id, agent_id, bucket_start(timestamp), value, false)
            }
            UserAgentStat::Overflow(_) => (
                service_id,
                agent_id,
                bucket_start(timestamp),
                OVERFLOW_MARKER.to_string(),
                true,
            ),
        };
        let entry = groups.entry(key).or_insert(UaGroupAggregate {
            message_count: 0,
            first_seen: timestamp,
            last_seen: timestamp,
        });
        entry.message_count += 1;
        entry.first_seen = entry.first_seen.min(timestamp);
        entry.last_seen = entry.last_seen.max(timestamp);
    }
    groups
}

/// Current committed state of one (service, agent, bucket) scope, read
/// inside the flush transaction.
#[derive(Debug, Clone)]
pub struct BucketCardinality {
    /// Distinct ordinary values already stored (capped read; the admission
    /// rule keeps this at MAX_DISTINCT_PER_BUCKET or below).
    pub ordinary_total: i64,
    /// Batch values that already exist as ordinary rows.
    pub known: HashSet<String>,
}

/// Enforce the per-bucket cardinality bound on grouped increments:
/// values already stored as ordinary rows keep updating; new distinct
/// values beyond the remaining capacity move to the bucket's overflow row.
/// New values beyond capacity are chosen in lexicographic order, so the
/// split does not depend on batch arrival order. Returns the number of
/// messages moved to overflow.
pub fn apply_cardinality_bound(
    groups: &mut HashMap<UaGroupKey, UaGroupAggregate>,
    service_id: i64,
    agent_id: &str,
    bucket: i64,
    cap: &BucketCardinality,
) -> u64 {
    let mut new_values: Vec<String> = groups
        .keys()
        .filter(|(svc, agt, bkt, ua, overflow)| {
            !*overflow
                && *svc == service_id
                && agt == agent_id
                && *bkt == bucket
                && !cap.known.contains(ua)
        })
        .map(|(_, _, _, ua, _)| ua.clone())
        .collect();
    if new_values.is_empty() {
        return 0;
    }
    new_values.sort();
    let capacity = (MAX_DISTINCT_PER_BUCKET - cap.ordinary_total).max(0) as usize;
    let overflowed = new_values.split_off(capacity.min(new_values.len()));
    if overflowed.is_empty() {
        return 0;
    }

    let mut moved = 0u64;
    for value in overflowed {
        let key = (service_id, agent_id.to_string(), bucket, value, false);
        if let Some(agg) = groups.remove(&key) {
            moved += agg.message_count as u64;
            let overflow_key = (
                service_id,
                agent_id.to_string(),
                bucket,
                OVERFLOW_MARKER.to_string(),
                true,
            );
            let entry = groups.entry(overflow_key).or_insert(UaGroupAggregate {
                message_count: 0,
                first_seen: agg.first_seen,
                last_seen: agg.last_seen,
            });
            entry.message_count += agg.message_count;
            entry.first_seen = entry.first_seen.min(agg.first_seen);
            entry.last_seen = entry.last_seen.max(agg.last_seen);
        }
    }
    moved
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(name: &str, value: &str) -> (String, String) {
        (name.to_string(), value.to_string())
    }

    #[test]
    fn header_name_matching_is_ascii_case_insensitive() {
        let headers = vec![header("USER-AGENT", "curl/8.0"), header("Host", "x")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Exact("curl/8.0".to_string())
        );
        let headers = vec![header("user-agent", "curl/8.0")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Exact("curl/8.0".to_string())
        );
    }

    #[test]
    fn responses_never_contribute() {
        let headers = vec![header("User-Agent", "curl/8.0")];
        assert_eq!(classify(&headers, false), UserAgentStat::None);
    }

    #[test]
    fn missing_empty_and_whitespace_only_values_do_not_contribute() {
        assert_eq!(classify(&[], true), UserAgentStat::None);
        assert_eq!(
            classify(&[header("User-Agent", "")], true),
            UserAgentStat::None
        );
        assert_eq!(
            classify(&[header("User-Agent", "  \t ")], true),
            UserAgentStat::None
        );
    }

    #[test]
    fn values_are_trimmed_of_http_optional_whitespace_only() {
        let headers = vec![header("User-Agent", "  curl/8.0\t")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Exact("curl/8.0".to_string())
        );
        // Interior whitespace and case are preserved byte-for-byte.
        let headers = vec![header("User-Agent", " App/1 (X11; Linux) ")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Exact("App/1 (X11; Linux)".to_string())
        );
    }

    #[test]
    fn control_characters_and_oversized_values_overflow() {
        let headers = vec![header("User-Agent", "bad\nvalue")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Overflow(UaOverflowReason::ControlChars)
        );
        let headers = vec![header("User-Agent", "bad\rvalue")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Overflow(UaOverflowReason::ControlChars)
        );
        let headers = vec![header("User-Agent", "bad\0value")];
        assert_eq!(
            classify(&headers, true),
            UserAgentStat::Overflow(UaOverflowReason::ControlChars)
        );
        let long = "a".repeat(MAX_VALUE_BYTES + 1);
        assert_eq!(
            classify(&[header("User-Agent", &long)], true),
            UserAgentStat::Overflow(UaOverflowReason::Oversized)
        );
        // Exactly at the bound is still an ordinary value.
        let at_bound = "a".repeat(MAX_VALUE_BYTES);
        assert_eq!(
            classify(&[header("User-Agent", &at_bound)], true),
            UserAgentStat::Exact(at_bound)
        );
    }

    #[test]
    fn bucket_start_floors_to_five_minutes() {
        assert_eq!(bucket_start(0), 0);
        assert_eq!(bucket_start(299), 0);
        assert_eq!(bucket_start(300), 300);
        assert_eq!(bucket_start(1_760_000_299), 1_760_000_100);
        assert_eq!(bucket_start(1_760_000_100), 1_760_000_100);
        assert_eq!(bucket_start(600), 600);
    }

    #[test]
    fn grouping_collapses_duplicates_and_tracks_first_last() {
        let groups = group_contributions(vec![
            (
                1,
                "agent-a".into(),
                1_000,
                UserAgentStat::Exact("curl".into()),
            ),
            (
                1,
                "agent-a".into(),
                1_100,
                UserAgentStat::Exact("curl".into()),
            ),
            (
                1,
                "agent-a".into(),
                1_050,
                UserAgentStat::Exact("curl".into()),
            ),
        ]);
        let key = (1, "agent-a".to_string(), 900, "curl".to_string(), false);
        let agg = groups.get(&key).expect("grouped");
        assert_eq!(agg.message_count, 3);
        assert_eq!(agg.first_seen, 1_000);
        assert_eq!(agg.last_seen, 1_100);
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn overflow_values_share_one_marker_row_per_bucket() {
        let groups = group_contributions(vec![
            (
                1,
                "a".into(),
                10,
                UserAgentStat::Overflow(UaOverflowReason::Oversized),
            ),
            (
                1,
                "a".into(),
                20,
                UserAgentStat::Overflow(UaOverflowReason::ControlChars),
            ),
        ]);
        let key = (1, "a".to_string(), 0, OVERFLOW_MARKER.to_string(), true);
        let agg = groups.get(&key).expect("overflow group");
        assert_eq!(agg.message_count, 2);
        assert_eq!(agg.first_seen, 10);
        assert_eq!(agg.last_seen, 20);
        // The overflow marker must never appear as an ordinary value.
        assert!(!groups.contains_key(&(1, "a".to_string(), 0, OVERFLOW_MARKER.to_string(), false)));
    }

    #[test]
    fn cardinality_bound_moves_only_new_values_beyond_capacity() {
        let mut groups = HashMap::new();
        for v in ["stored-1", "stored-2", "new-a", "new-b", "new-c"] {
            groups.insert(
                (1i64, "a".to_string(), 0i64, v.to_string(), false),
                UaGroupAggregate {
                    message_count: 1,
                    first_seen: 5,
                    last_seen: 5,
                },
            );
        }
        let cap = BucketCardinality {
            ordinary_total: MAX_DISTINCT_PER_BUCKET - 1,
            known: ["stored-1", "stored-2"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        };

        let moved = apply_cardinality_bound(&mut groups, 1, "a", 0, &cap);

        assert_eq!(moved, 2);
        // Known values and the first new value in lexicographic order stay.
        for kept in ["stored-1", "stored-2", "new-a"] {
            assert!(
                groups.contains_key(&(1, "a".to_string(), 0, kept.to_string(), false)),
                "{kept} stays ordinary"
            );
        }
        // The rest collapsed into the overflow row.
        let overflow = groups
            .get(&(1, "a".to_string(), 0, OVERFLOW_MARKER.to_string(), true))
            .expect("overflow row");
        assert_eq!(overflow.message_count, 2);
    }

    #[test]
    fn saturated_bucket_sends_every_new_value_to_overflow() {
        let mut groups = HashMap::new();
        groups.insert(
            (1, "a".to_string(), 0, "fresh".to_string(), false),
            UaGroupAggregate {
                message_count: 4,
                first_seen: 1,
                last_seen: 4,
            },
        );
        let cap = BucketCardinality {
            ordinary_total: MAX_DISTINCT_PER_BUCKET,
            known: HashSet::new(),
        };

        let moved = apply_cardinality_bound(&mut groups, 1, "a", 0, &cap);

        assert_eq!(moved, 4);
        assert!(!groups.contains_key(&(1, "a".to_string(), 0, "fresh".to_string(), false)));
        assert_eq!(
            groups
                .get(&(1, "a".to_string(), 0, OVERFLOW_MARKER.to_string(), true))
                .expect("overflow row")
                .message_count,
            4
        );
    }

    #[test]
    fn known_values_continue_updating_after_saturation() {
        let mut groups = HashMap::new();
        groups.insert(
            (1, "a".to_string(), 0, "kept".to_string(), false),
            UaGroupAggregate {
                message_count: 7,
                first_seen: 1,
                last_seen: 7,
            },
        );
        let cap = BucketCardinality {
            ordinary_total: MAX_DISTINCT_PER_BUCKET,
            known: ["kept".to_string()].into_iter().collect(),
        };

        let moved = apply_cardinality_bound(&mut groups, 1, "a", 0, &cap);

        assert_eq!(moved, 0);
        assert!(groups.contains_key(&(1, "a".to_string(), 0, "kept".to_string(), false)));
    }
}
