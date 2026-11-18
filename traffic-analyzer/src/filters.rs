pub mod bsubstr;
pub mod filterDTO;
pub mod regex;
pub mod spec;
pub mod substr;

use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::filterDTO::{Filter, FilterDirection, FilterTrigger};
use crate::filters::regex::RegexFilter;
use crate::filters::spec::{Action, FilterSpec, IpEndpointSel, MatcherSpec};
use crate::filters::substr::SubstrFilter;
use crate::process_packet::Direction;
use filter_dsl::EvalInput;
use log::{debug, error, warn};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::RwLock;

/// Per-packet evaluation context. src/dst are the ORIGINAL packet
/// endpoints (before stream tuple normalization); direction and machine_ip
/// are relative to the capturing agent.
pub struct FilterContext<'a> {
    pub payload: &'a [u8],
    pub direction: Direction,
    pub agent_id: &'a str,
    pub service_id: u64,
    pub stream_id: u64,
    pub src_ip: IpAddr,
    pub dst_ip: IpAddr,
    pub src_port: u16,
    pub dst_port: u16,
    pub machine_ip: IpAddr,
    /// "TCP" or "UDP"
    pub protocol: &'a str,
    pub timestamp_secs: u64,
    /// Wall clock, epoch millis (expiry checks).
    pub now_ms: u64,
}

/// One matching filter's outcome: tag offsets (payload matchers), plus the
/// metadata the action paths need.
pub struct FilterMatch {
    pub filter_id: u64,
    /// Offsets for payload matchers; empty for IP and expression matches
    /// (their tag rows carry no offsets and are emitted separately).
    pub offsets: Vec<FilterTrigger>,
    pub offset_tag: bool,
    pub action: Action,
    pub revision: u64,
    pub threshold: Option<spec::ThresholdCfg>,
}

pub(crate) enum Matcher {
    Payload(Box<dyn Filter>),
    IpCidr {
        network: IpAddr,
        prefix: u8,
        endpoint: IpEndpointSel,
    },
    Expression(filter_dsl::Expr),
}

/// A registered unified filter.
struct FilterEntry {
    spec: FilterSpec,
    matcher: Matcher,
}

impl FilterEntry {
    fn expired(&self, now_ms: u64) -> bool {
        self.spec.expires_at_ms != 0 && self.spec.expires_at_ms <= now_ms
    }
}

// Global storage for filters.
static PACKET_FILTERS: Lazy<RwLock<Vec<FilterEntry>>> = Lazy::new(|| RwLock::new(Vec::new()));

/// Pure direction check: does a filter with this direction apply to a
/// packet with this packet direction?
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

/// Compile a spec's matcher. Returns None with a logged reason for specs
/// this build cannot run faithfully.
fn build_matcher(spec: &FilterSpec) -> Option<Matcher> {
    match &spec.matcher {
        MatcherSpec::Payload { kind, payload } => {
            let id = usize::try_from(spec.id).ok()?;
            let filter: Box<dyn Filter> = match kind.to_lowercase().as_str() {
                "regex" => {
                    let payload_str = match String::from_utf8(payload.clone()) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Filter '{}': payload is not UTF-8: {}", spec.name, e);
                            return None;
                        }
                    };
                    match RegexFilter::new(id, &spec.name, &payload_str) {
                        Ok(filter) => Box::new(filter),
                        Err(e) => {
                            error!("Filter '{}': bad regex: {}", spec.name, e);
                            return None;
                        }
                    }
                }
                // bsubstr templates are HEX strings; the matcher searches
                // for the DECODED bytes. Invalid hex falls back to raw
                // bytes only for legacy rows (new rows are validated at
                // the backend).
                "bsubstr" => {
                    let pattern_bytes = match hex::decode(payload) {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            warn!(
                                "Filter '{}': payload is not valid hex, using raw bytes",
                                spec.name
                            );
                            payload.clone()
                        }
                    };
                    Box::new(BinarySubstrFilter::new(id, &spec.name, &pattern_bytes))
                }
                "substr" => Box::new(SubstrFilter::new(id, &spec.name, payload)),
                other => {
                    error!(
                        "Filter '{}': unsupported matcher kind '{}'",
                        spec.name, other
                    );
                    return None;
                }
            };
            Some(Matcher::Payload(filter))
        }
        MatcherSpec::IpCidr {
            network,
            prefix,
            endpoint,
        } => Some(Matcher::IpCidr {
            network: *network,
            prefix: *prefix,
            endpoint: *endpoint,
        }),
        MatcherSpec::Expression { source } => match filter_dsl::compile(source) {
            Ok(expr) => Some(Matcher::Expression(expr)),
            Err(e) => {
                error!(
                    "Filter '{}': expression failed to compile: {}",
                    spec.name, e
                );
                None
            }
        },
    }
}

/// Register (or replace) a filter. Idempotent by id with a revision gate:
/// an update older than the registered revision is ignored (a stale
/// JetStream redelivery must never roll a newer definition back).
/// Returns true when the filter was registered, false when rejected
/// (reason logged).
pub fn add_filter(spec: FilterSpec) -> bool {
    let Some(matcher) = build_matcher(&spec) else {
        return false;
    };
    let mut filters = PACKET_FILTERS.write().unwrap();
    if let Some(existing) = filters.iter().find(|e| e.spec.id == spec.id) {
        if spec.revision < existing.spec.revision {
            debug!(
                "Filter id {} revision {} older than registered {}, ignored",
                spec.id, spec.revision, existing.spec.revision
            );
            return false;
        }
    }
    // Drop any stale entry with the same id, then insert the fresh one.
    filters.retain(|entry| entry.spec.id != spec.id);
    filters.push(FilterEntry { spec, matcher });
    true
}

pub fn remove_filter(id: u64) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    if let Some(position) = filters.iter().position(|entry| entry.spec.id == id) {
        filters.remove(position);
        debug!("Filter with id {} removed successfully", id);
    } else {
        debug!("Filter with id {} not found (already removed)", id);
    }
}

pub fn edit_filter(spec: FilterSpec) {
    remove_filter(spec.id);
    add_filter(spec);
}

pub fn get_filter_ids() -> Vec<u64> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters.iter().map(|entry| entry.spec.id).collect()
}

/// Current revision of a registered filter, or None when absent. Used to
/// re-validate a threshold block at persistence time.
pub fn filter_revision(id: u64) -> Option<u64> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters
        .iter()
        .find(|entry| entry.spec.id == id)
        .map(|entry| entry.spec.revision)
}

/// (filter_id, window_secs, cooldown_secs) of every registered threshold
/// filter, for the idle sweep.
pub fn threshold_configs_snapshot() -> Vec<(u64, u32, u32)> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters
        .iter()
        .filter_map(|entry| {
            entry
                .spec
                .threshold
                .as_ref()
                .map(|t| (entry.spec.id, t.window_secs, t.cooldown_secs))
        })
        .collect()
}

/// Snapshot of filter_id -> tag name (tag_name.name in the DB). Used by
/// the batch flusher to maintain stream_tag with the SAME strings the old
/// materialized view produced.
pub fn filter_names_snapshot() -> HashMap<u64, String> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters
        .iter()
        .map(|entry| (entry.spec.id, entry.spec.name.clone()))
        .collect()
}

/// Full replacement with the current DB view (ConfigSync / warm-up
/// reconciliation). Per-id revision gates apply: entries whose new
/// revision is older than the registered one keep the registered state.
pub fn sync_filters(specs: Vec<FilterSpec>) {
    let incoming_ids: std::collections::HashSet<u64> = specs.iter().map(|s| s.id).collect();
    let existing_ids: std::collections::HashSet<u64> = get_filter_ids().into_iter().collect();

    let mut registered = 0usize;
    for spec in &specs {
        if add_filter(spec.clone()) {
            registered += 1;
        }
    }
    let to_remove: Vec<u64> = existing_ids.difference(&incoming_ids).copied().collect();
    for id in to_remove {
        remove_filter(id);
    }
    debug!(
        "Filters sync: {} specs, {} registered, {} stale removed",
        specs.len(),
        registered,
        incoming_ids.len()
    );
}

fn ip_endpoint_matches(
    endpoint: IpEndpointSel,
    ctx: &FilterContext,
    network: IpAddr,
    prefix: u8,
) -> bool {
    let addr = match endpoint {
        IpEndpointSel::Src => ctx.src_ip,
        IpEndpointSel::Dst => ctx.dst_ip,
    };
    // Families never mix: an IPv4 rule never matches an IPv6 endpoint.
    if addr.is_ipv4() != network.is_ipv4() {
        return false;
    }
    prefix_contains(addr, network, prefix)
}

fn prefix_contains(addr: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (addr, network) {
        (IpAddr::V4(a), IpAddr::V4(n)) => {
            let abits = u32::from(a);
            let nbits = u32::from(n);
            let mask = if prefix == 0 {
                0
            } else if prefix >= 32 {
                u32::MAX
            } else {
                u32::MAX << (32 - prefix)
            };
            (abits & mask) == (nbits & mask)
        }
        (IpAddr::V6(a), IpAddr::V6(n)) => {
            let abits = u128::from(a);
            let nbits = u128::from(n);
            let mask = if prefix == 0 {
                0
            } else if prefix >= 128 {
                u128::MAX
            } else {
                u128::MAX << (128 - prefix)
            };
            (abits & mask) == (nbits & mask)
        }
        _ => false,
    }
}

/// IP and expression matches emit a tag WITHOUT payload offsets; the tag
/// row carries (0, 0) so packet view anchors stay payload-only.
pub const NO_OFFSET: (usize, usize) = (0, 0);

/// Apply all applicable filters to one packet. OR semantics: every
/// applicable filter is evaluated and every match is emitted; evaluation
/// never stops at the first match. A failed expression (or exhausted
/// budget) is a NON-MATCH for that filter only.
pub fn apply_filters(ctx: &FilterContext<'_>) -> Vec<FilterMatch> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut results: Vec<FilterMatch> = Vec::new();

    // The expression input costs string allocations; build it lazily and
    // only once per packet, only when an expression filter applies. The
    // strings live in expr_strings so the borrowed EvalInput stays valid.
    #[allow(unused_assignments)] // the None init is the reset point per packet
    let mut expr_strings: Option<(String, String, String, String)> = None;
    let mut expr_input: Option<EvalInput<'_>> = None;

    for entry in filters.iter() {
        let spec = &entry.spec;
        if !spec.enabled || entry.expired(ctx.now_ms) {
            continue;
        }
        if !direction_matches(&spec.direction, &ctx.direction) {
            continue;
        }
        if !spec.targets.service_applies(ctx.service_id)
            || !spec.targets.agent_applies(ctx.agent_id)
        {
            continue;
        }

        let threshold = spec.threshold.clone();
        let action = spec.action;
        let revision = spec.revision;
        let filter_id = spec.id;

        match &entry.matcher {
            Matcher::Payload(filter) => {
                let offsets = filter.processed(ctx.payload);
                if offsets.is_empty() {
                    continue;
                }
                results.push(FilterMatch {
                    filter_id,
                    offsets,
                    offset_tag: true,
                    action,
                    revision,
                    threshold,
                });
            }
            Matcher::IpCidr {
                network,
                prefix,
                endpoint,
            } => {
                if ip_endpoint_matches(*endpoint, ctx, *network, *prefix) {
                    results.push(FilterMatch {
                        filter_id,
                        offsets: vec![FilterTrigger {
                            start_index: NO_OFFSET.0,
                            end_index: NO_OFFSET.1,
                        }],
                        offset_tag: false,
                        action,
                        revision,
                        threshold,
                    });
                }
            }
            Matcher::Expression(expr) => {
                if expr_input.is_none() {
                    let src = ctx.src_ip.to_string();
                    let dst = ctx.dst_ip.to_string();
                    let local = ctx.machine_ip.to_string();
                    let remote = remote_ip_of(ctx).to_string();
                    expr_strings = Some((src, dst, local, remote));
                    let (src, dst, local, remote) = expr_strings.as_ref().unwrap();
                    expr_input = Some(EvalInput {
                        payload: ctx.payload,
                        src_ip: src,
                        dst_ip: dst,
                        src_port: u64::from(ctx.src_port),
                        dst_port: u64::from(ctx.dst_port),
                        local_ip: local,
                        remote_ip: remote,
                        local_port: u64::from(local_port_of(ctx)),
                        remote_port: u64::from(remote_port_of(ctx)),
                        protocol: ctx.protocol,
                        direction: match ctx.direction {
                            Direction::Inbound => "in",
                            Direction::Outbound => "out",
                        },
                        service_id: ctx.service_id,
                        agent_id: ctx.agent_id,
                        timestamp: ctx.timestamp_secs,
                    });
                }
                let input = expr_input.as_ref().unwrap();
                match filter_dsl::eval(expr, input) {
                    Ok(true) => results.push(FilterMatch {
                        filter_id,
                        offsets: vec![FilterTrigger {
                            start_index: NO_OFFSET.0,
                            end_index: NO_OFFSET.1,
                        }],
                        offset_tag: false,
                        action,
                        revision,
                        threshold,
                    }),
                    // Error (limit exhaustion, invalid dynamic IP) is a
                    // NON-MATCH for this filter only; other filters and
                    // packets continue.
                    Ok(false) => {}
                    Err(e) => {
                        crate::metrics::EXPRESSION_EVAL_ERRORS.fetch_add(1, Ordering::Relaxed);
                        let total = crate::metrics::EXPRESSION_EVAL_ERRORS.load(Ordering::Relaxed);
                        crate::metrics::rate_limited_log("expression-error", total, || {
                            format!("filter {} expression error: {}", filter_id, e)
                        });
                    }
                }
            }
        }
    }

    results
}

pub(crate) fn remote_ip_of(ctx: &FilterContext<'_>) -> IpAddr {
    match ctx.direction {
        Direction::Inbound => ctx.src_ip,
        Direction::Outbound => ctx.dst_ip,
    }
}

fn local_port_of(ctx: &FilterContext<'_>) -> u16 {
    match ctx.direction {
        Direction::Inbound => ctx.dst_port,
        Direction::Outbound => ctx.src_port,
    }
}

fn remote_port_of(ctx: &FilterContext<'_>) -> u16 {
    match ctx.direction {
        Direction::Inbound => ctx.src_port,
        Direction::Outbound => ctx.dst_port,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::spec::{GroupBy, Targets, ThresholdCfg};

    /// PACKET_FILTERS is a process-global: tests that mutate it must not
    /// run concurrently.
    static FILTERS_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn ctx<'a>(payload: &'a [u8], direction: Direction) -> FilterContext<'a> {
        FilterContext {
            payload,
            direction,
            agent_id: "agent-a",
            service_id: 1,
            stream_id: 1,
            src_ip: "10.0.0.9".parse().unwrap(),
            dst_ip: "10.0.0.5".parse().unwrap(),
            src_port: 40000,
            dst_port: 80,
            machine_ip: "10.0.0.5".parse().unwrap(),
            protocol: "TCP",
            timestamp_secs: 1_700_000_000,
            now_ms: 0,
        }
    }

    fn payload_spec(id: u64, kind: &str, pattern: &[u8], direction: FilterDirection) -> FilterSpec {
        FilterSpec {
            id,
            name: format!("f-{}", id),
            direction,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            matcher: MatcherSpec::Payload {
                kind: kind.to_string(),
                payload: pattern.to_vec(),
            },
        }
    }

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
        let id = 987_654;
        add_filter(payload_spec(
            id,
            "substr",
            b"flag",
            FilterDirection::Inbound,
        ));

        let payload = b"flag{xxx}";
        let hit = apply_filters(&ctx(payload, Direction::Inbound));
        assert!(hit
            .iter()
            .any(|m| m.filter_id == id && !m.offsets.is_empty()));
        let miss = apply_filters(&ctx(payload, Direction::Outbound));
        assert!(!miss.iter().any(|m| m.filter_id == id));

        // Both -> fires in either direction.
        let mut both = payload_spec(id, "substr", b"flag", FilterDirection::Both);
        both.revision += 1;
        edit_filter(both);
        assert!(apply_filters(&ctx(payload, Direction::Inbound))
            .iter()
            .any(|m| m.filter_id == id));
        assert!(apply_filters(&ctx(payload, Direction::Outbound))
            .iter()
            .any(|m| m.filter_id == id));
        remove_filter(id);
    }

    /// OR semantics: two independent filters each emit their own match and
    /// their own offsets; evaluation never stops at the first match.
    #[test]
    fn two_filters_emit_every_match() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let a = 987_670;
        let b = 987_671;
        add_filter(payload_spec(a, "substr", b"flag", FilterDirection::Both));
        add_filter(payload_spec(b, "substr", b"xxx", FilterDirection::Both));

        let payload = b"flag{xxx}";
        let hits = apply_filters(&ctx(payload, Direction::Inbound));
        let ma = hits.iter().find(|m| m.filter_id == a).unwrap();
        let mb = hits.iter().find(|m| m.filter_id == b).unwrap();
        assert_eq!(ma.offsets.len(), 1);
        assert_eq!(mb.offsets.len(), 1);

        remove_filter(a);
        remove_filter(b);
    }

    /// add_filter is idempotent by id (replace, never duplicate), and a
    /// stale revision cannot roll back a newer definition.
    #[test]
    fn add_filter_is_idempotent_and_revision_gated() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_655;
        add_filter(payload_spec(id, "substr", b"old", FilterDirection::Both));
        let mut stale = payload_spec(id, "substr", b"stale", FilterDirection::Both);
        stale.revision = 0;
        add_filter(stale);
        assert_eq!(get_filter_ids().iter().filter(|&&fid| fid == id).count(), 1);
        // The newer payload survived the stale update.
        assert!(apply_filters(&ctx(b"old", Direction::Inbound))
            .iter()
            .any(|m| m.filter_id == id));
        assert!(!apply_filters(&ctx(b"stale", Direction::Inbound))
            .iter()
            .any(|m| m.filter_id == id));
        remove_filter(id);
    }

    #[test]
    fn sync_filters_right_after_warmup_does_not_duplicate() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_656;
        add_filter(payload_spec(
            id,
            "substr",
            b"warm",
            FilterDirection::Inbound,
        ));
        sync_filters(vec![payload_spec(
            id,
            "substr",
            b"warm",
            FilterDirection::Inbound,
        )]);
        assert_eq!(get_filter_ids().iter().filter(|&&fid| fid == id).count(), 1);
        remove_filter(id);
    }

    #[test]
    fn sync_filters_converges_after_db_change() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let stale_id = 987_657;
        let live_id = 987_658;
        add_filter(payload_spec(
            stale_id,
            "substr",
            b"stale",
            FilterDirection::Both,
        ));
        add_filter(payload_spec(
            live_id,
            "substr",
            b"live",
            FilterDirection::Both,
        ));

        sync_filters(vec![payload_spec(
            live_id,
            "substr",
            b"live",
            FilterDirection::Both,
        )]);
        let ids = get_filter_ids();
        assert!(!ids.contains(&stale_id));
        assert_eq!(ids.iter().filter(|&&fid| fid == live_id).count(), 1);
        remove_filter(live_id);
    }

    /// bsubstr templates arrive as HEX strings and match decoded bytes,
    /// not the hex text.
    #[test]
    fn bsubstr_hex_template_matches_decoded_bytes() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_660;
        add_filter(payload_spec(
            id,
            "bsubstr",
            b"deadbeef",
            FilterDirection::Both,
        ));

        let hit = apply_filters(&ctx(
            &[0x00, 0xde, 0xad, 0xbe, 0xef, 0x11],
            Direction::Inbound,
        ));
        assert!(hit.iter().any(|m| m.filter_id == id));
        let miss = apply_filters(&ctx(b"xxdeadbeefyy", Direction::Inbound));
        assert!(!miss.iter().any(|m| m.filter_id == id));
        remove_filter(id);
    }

    #[test]
    fn bsubstr_non_hex_payload_falls_back_to_raw_bytes() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_661;
        add_filter(payload_spec(
            id,
            "bsubstr",
            b"zz not hex zz",
            FilterDirection::Both,
        ));
        let hit = apply_filters(&ctx(b"prefix zz not hex zz suffix", Direction::Outbound));
        assert!(hit.iter().any(|m| m.filter_id == id));
        remove_filter(id);
    }

    /// Edit replaces payload and direction (post-warm-up FilterUpdate).
    #[test]
    fn edit_filter_after_warmup_replaces_not_duplicates() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_659;
        add_filter(payload_spec(id, "substr", b"old", FilterDirection::Both));
        let mut updated = payload_spec(id, "substr", b"new", FilterDirection::Inbound);
        updated.revision += 1;
        edit_filter(updated);
        assert_eq!(get_filter_ids().iter().filter(|&&fid| fid == id).count(), 1);
        assert!(apply_filters(&ctx(b"new", Direction::Inbound))
            .iter()
            .any(|m| m.filter_id == id));
        assert!(!apply_filters(&ctx(b"new", Direction::Outbound))
            .iter()
            .any(|m| m.filter_id == id));
        remove_filter(id);
    }

    /// IP/CIDR matcher: src endpoint matches the ORIGINAL packet's source
    /// address in both stream orientations; v4 never matches v6.
    #[test]
    fn ip_cidr_matches_original_endpoints_in_both_orientations() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_680;
        let spec = FilterSpec {
            id,
            name: "ip-src".into(),
            direction: FilterDirection::Both,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            matcher: MatcherSpec::IpCidr {
                network: "10.0.0.9".parse().unwrap(),
                prefix: 32,
                endpoint: IpEndpointSel::Src,
            },
        };
        add_filter(spec);

        // Inbound: 10.0.0.9:40000 -> 10.0.0.5:80; src is the remote client.
        let inbound = apply_filters(&ctx(b"x", Direction::Inbound));
        assert!(inbound.iter().any(|m| m.filter_id == id));
        // Outbound: 10.0.0.5:80 -> 10.0.0.9:40000; src is the local machine.
        let outbound = apply_filters(&ctx(b"x", Direction::Outbound));
        assert!(outbound.iter().any(|m| m.filter_id == id));

        // A packet whose src is NOT the network must not match.
        let mut other = ctx(b"x", Direction::Inbound);
        other.src_ip = "10.0.1.9".parse().unwrap();
        assert!(!apply_filters(&other).iter().any(|m| m.filter_id == id));

        remove_filter(id);
    }

    /// IPv4 rules never match IPv6 endpoints.
    #[test]
    fn ip_cidr_never_matches_across_families() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_681;
        add_filter(FilterSpec {
            id,
            name: "ip-v4".into(),
            direction: FilterDirection::Both,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            matcher: MatcherSpec::IpCidr {
                network: "10.0.0.9".parse().unwrap(),
                prefix: 32,
                endpoint: IpEndpointSel::Src,
            },
        });
        let mut v6 = ctx(b"x", Direction::Inbound);
        v6.src_ip = "::ffff:10.0.0.9".parse::<IpAddr>().unwrap();
        // "::ffff:10.0.0.9" parses as an IPv6 address - must NOT match the
        // v4 rule.
        assert!(v6.src_ip.is_ipv6());
        assert!(!apply_filters(&v6).iter().any(|m| m.filter_id == id));
        remove_filter(id);
    }

    /// Expression matcher evaluates against the packet context, errors are
    /// non-matches, and disabled/expired/out-of-target filters skip.
    #[test]
    fn expression_filter_evaluates_and_fails_closed_per_filter() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let hit_id = 987_690;
        let miss_id = 987_691;
        add_filter(FilterSpec {
            id: hit_id,
            name: "expr".into(),
            direction: FilterDirection::Both,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            matcher: MatcherSpec::Expression {
                source: "protocol == \"TCP\" && ip_in_cidr(remote_ip, \"10.0.0.0/8\") && payload.contains(b\"flag\")".into(),
            },
        });
        add_filter(FilterSpec {
            id: miss_id,
            name: "expr-error".into(),
            direction: FilterDirection::Both,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            // agent_id is not an IP: the dynamic ip_in_cidr argument fails
            // at evaluation -> NON-MATCH for this filter only.
            matcher: MatcherSpec::Expression {
                source: "ip_in_cidr(agent_id, \"10.0.0.0/8\")".into(),
            },
        });

        let hits = apply_filters(&ctx(b"flag here", Direction::Inbound));
        assert!(hits.iter().any(|m| m.filter_id == hit_id));
        assert!(!hits.iter().any(|m| m.filter_id == miss_id));
        // Expression matches carry no payload offsets.
        let hit = hits.iter().find(|m| m.filter_id == hit_id).unwrap();
        assert!(!hit.offset_tag);

        remove_filter(hit_id);
        remove_filter(miss_id);
    }

    #[test]
    fn disabled_or_expired_or_out_of_target_filters_skip() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let base = 987_700;
        let mut disabled = payload_spec(base, "substr", b"flag", FilterDirection::Both);
        disabled.enabled = false;
        add_filter(disabled);

        let mut expired = payload_spec(base + 1, "substr", b"flag", FilterDirection::Both);
        expired.expires_at_ms = 1;
        add_filter(expired);

        let mut targeted = payload_spec(base + 2, "substr", b"flag", FilterDirection::Both);
        targeted.targets = Targets {
            services_all: false,
            service_ids: [99u64].into_iter().collect(),
            agents_all: false,
            agent_ids: ["other-agent".to_string()].into_iter().collect(),
        };
        add_filter(targeted);

        let payload = b"flag{xxx}";
        let mut check = ctx(payload, Direction::Inbound);
        check.now_ms = 1_700_000_000_000; // the 1ms expiry is long past
        let hits = apply_filters(&check);
        assert!(hits.is_empty(), "none of the three filters may match");

        for id in base..=base + 2 {
            remove_filter(id);
        }
    }

    /// Direction/endpoint contract for UDP as well: src/dst semantics are
    /// transport-independent.
    #[test]
    fn udp_packets_follow_the_same_endpoint_contract() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_720;
        add_filter(FilterSpec {
            id,
            name: "udp-dst".into(),
            direction: FilterDirection::Both,
            action: Action::Tag,
            enabled: true,
            revision: 1,
            targets: Targets::all(),
            threshold: None,
            expires_at_ms: 0,
            matcher: MatcherSpec::IpCidr {
                network: "10.0.0.5".parse().unwrap(),
                prefix: 32,
                endpoint: IpEndpointSel::Dst,
            },
        });
        // Inbound UDP: dst is the local machine (10.0.0.5) -> match.
        let mut udp_in = ctx(b"x", Direction::Inbound);
        udp_in.protocol = "UDP";
        assert!(apply_filters(&udp_in).iter().any(|m| m.filter_id == id));
        // Outbound UDP (local 10.0.0.5 -> remote 10.0.0.9): dst is the
        // remote -> no match.
        let mut udp_out = ctx(b"x", Direction::Outbound);
        udp_out.protocol = "UDP";
        udp_out.src_ip = "10.0.0.5".parse().unwrap();
        udp_out.dst_ip = "10.0.0.9".parse().unwrap();
        assert!(!apply_filters(&udp_out).iter().any(|m| m.filter_id == id));
        remove_filter(id);
    }

    #[test]
    fn threshold_config_flows_through_the_match() {
        let _guard = FILTERS_TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let id = 987_710;
        let mut spec = payload_spec(id, "substr", b"flag", FilterDirection::Both);
        spec.action = Action::Block;
        spec.threshold = Some(ThresholdCfg {
            count: 3,
            window_secs: 10,
            group_by: GroupBy::RemoteIp,
            cooldown_secs: 30,
            block_secs: 60,
        });
        add_filter(spec);
        let hits = apply_filters(&ctx(b"flag", Direction::Inbound));
        let hit = hits.iter().find(|m| m.filter_id == id).unwrap();
        assert_eq!(hit.action, Action::Block);
        let threshold = hit.threshold.as_ref().unwrap();
        assert_eq!(threshold.count, 3);
        assert_eq!(threshold.block_secs, 60);
        remove_filter(id);
    }
}
