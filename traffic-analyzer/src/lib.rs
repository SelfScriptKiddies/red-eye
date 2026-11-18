pub mod batch_flusher;
pub mod cache;
pub mod configuration;
pub mod db;
pub mod ethernet;
pub mod filter_events;
pub mod filters;
pub mod grpc_parser;
pub mod http_parser;
pub mod message_broker;
pub mod metrics;
pub mod process_packet;
pub mod proto_schema;
pub mod retention;
pub mod thresholds;
pub mod user_agent;
pub mod version;
pub mod wire;
pub mod ws_aggregator;
use crate::configuration::init;
use log::{debug, error, info};
use std::sync::Arc;

/// Load proto files from DB and populate the proto_schemas cache.
async fn load_proto_schemas(db: &db::DB, cache: &cache::Cache) {
    let proto_files = db.get_all_proto_files().await;
    let mut schemas = cache.proto_schemas.write().unwrap();
    schemas.clear();
    for (service_id, files) in proto_files {
        let parsed: Vec<proto_schema::ProtoSchema> = files
            .iter()
            .map(|(_, content)| proto_schema::parse_proto(content))
            .collect();
        let merged = proto_schema::merge_protos(&parsed);
        info!(
            "Loaded proto for service {}: {} messages, {} services",
            service_id,
            merged.messages.len(),
            merged.methods.len()
        );
        schemas.insert(service_id as usize, merged);
    }
}

/// tag_name row shape for the filter warm-up (sqlx tuple support stops at
/// 16 columns; the unified model has 19).
#[derive(sqlx::FromRow)]
struct FilterModelRow {
    id: i64,
    name: String,
    template_type: String,
    template: String,
    direction: String,
    action: String,
    enabled: bool,
    ip_network: Option<sqlx::types::ipnetwork::IpNetwork>,
    ip_endpoint: Option<String>,
    expression_source: Option<String>,
    service_targets_global: bool,
    agent_targets_global: bool,
    threshold_count: Option<i32>,
    threshold_window_secs: Option<i32>,
    threshold_group_by: Option<String>,
    threshold_cooldown_secs: Option<i32>,
    threshold_block_secs: Option<i32>,
    expires_at_ms: Option<i64>,
    revision: i64,
}

/// Load the full unified filter model (spec + explicit targets) from the
/// tag_name tables. Returns one FilterSpec per row; rows this build cannot
/// interpret faithfully (unknown action, bad CIDR) are skipped and logged.
async fn load_filter_specs_from_db(db: &db::DB) -> Vec<filters::spec::FilterSpec> {
    let rows = sqlx::query_as::<_, FilterModelRow>(
        "SELECT id, name, template_type, template, direction, action, enabled, \
         ip_network, ip_endpoint, expression_source, service_targets_global, \
         agent_targets_global, threshold_count, threshold_window_secs, threshold_group_by, \
         threshold_cooldown_secs, threshold_block_secs, \
         CASE WHEN expires_at IS NULL THEN NULL \
              ELSE (EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT END AS expires_at_ms, \
         revision \
         FROM tag_name ORDER BY name",
    )
    .fetch_all(db.pool())
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            error!(
                "Failed to load filters from DB, continuing with empty filters \
                 (reconciliation and ConfigSync will recover): {}",
                e
            );
            return Vec::new();
        }
    };

    let mut service_targets: std::collections::HashMap<i64, Vec<i64>> =
        std::collections::HashMap::new();
    let mut agent_targets: std::collections::HashMap<i64, Vec<String>> =
        std::collections::HashMap::new();
    if let Ok(pairs) =
        sqlx::query_as::<_, (i64, i64)>("SELECT filter_id, service_id FROM filter_service_target")
            .fetch_all(db.pool())
            .await
    {
        for (filter_id, service_id) in pairs {
            service_targets
                .entry(filter_id)
                .or_default()
                .push(service_id);
        }
    }
    if let Ok(pairs) =
        sqlx::query_as::<_, (i64, String)>("SELECT filter_id, agent_id FROM filter_agent_target")
            .fetch_all(db.pool())
            .await
    {
        for (filter_id, agent_id) in pairs {
            agent_targets.entry(filter_id).or_default().push(agent_id);
        }
    }

    let mut specs = Vec::new();
    for row in rows {
        let id = row.id;
        let spec = build_spec_from_row(
            row.id,
            row.name,
            row.template_type,
            row.template,
            row.direction,
            row.action,
            row.enabled,
            row.ip_network,
            row.ip_endpoint,
            row.expression_source,
            row.service_targets_global,
            row.agent_targets_global,
            row.threshold_count,
            row.threshold_window_secs,
            row.threshold_group_by,
            row.threshold_cooldown_secs,
            row.threshold_block_secs,
            row.expires_at_ms,
            row.revision,
            service_targets.remove(&id).unwrap_or_default(),
            agent_targets.remove(&id).unwrap_or_default(),
        );
        match spec {
            Some(spec) => specs.push(spec),
            None => debug!("Skipping filter id {} that cannot be mapped safely", id),
        }
    }
    specs
}

/// Pure row -> FilterSpec mapping, extracted so the row semantics are
/// unit-testable without a database. Mirrors the backend wire encoding.
#[allow(clippy::too_many_arguments)]
fn build_spec_from_row(
    id: i64,
    name: String,
    template_type: String,
    template: String,
    direction: String,
    action: String,
    enabled: bool,
    ip_network: Option<sqlx::types::ipnetwork::IpNetwork>,
    ip_endpoint: Option<String>,
    expression_source: Option<String>,
    service_targets_global: bool,
    agent_targets_global: bool,
    threshold_count: Option<i32>,
    threshold_window_secs: Option<i32>,
    threshold_group_by: Option<String>,
    threshold_cooldown_secs: Option<i32>,
    threshold_block_secs: Option<i32>,
    expires_at_ms: Option<i64>,
    revision: i64,
    service_ids: Vec<i64>,
    agent_ids: Vec<String>,
) -> Option<filters::spec::FilterSpec> {
    use filters::spec::{Action, GroupBy, IpEndpointSel, MatcherSpec, Targets, ThresholdCfg};

    let id = u64::try_from(id).ok()?;
    let direction = filters::filterDTO::FilterDirection::from_tag_direction(&direction);
    let action = match action.as_str() {
        "tag" => Action::Tag,
        "flag" => Action::Flag,
        "alert" => Action::Alert,
        "block" => Action::Block,
        "block_connection" => Action::BlockConnection,
        _ => return None,
    };
    let matcher = match template_type.as_str() {
        "ip_cidr" => {
            let net = ip_network?;
            let endpoint = match ip_endpoint.as_deref() {
                Some("dst") => IpEndpointSel::Dst,
                Some("src") => IpEndpointSel::Src,
                _ => return None,
            };
            MatcherSpec::IpCidr {
                network: net.ip(),
                prefix: net.prefix(),
                endpoint,
            }
        }
        "expression" => MatcherSpec::Expression {
            source: expression_source?,
        },
        other => MatcherSpec::Payload {
            kind: other.to_string(),
            payload: template.into_bytes(),
        },
    };
    let threshold = match (threshold_count, threshold_window_secs) {
        (Some(count), Some(window)) if count > 0 && window > 0 => {
            let group_by = match threshold_group_by.as_deref() {
                Some("stream") => GroupBy::Stream,
                Some("service") => GroupBy::Service,
                _ => GroupBy::RemoteIp,
            };
            Some(ThresholdCfg {
                count: count as u32,
                window_secs: window as u32,
                group_by,
                cooldown_secs: threshold_cooldown_secs.unwrap_or(0).max(0) as u32,
                block_secs: threshold_block_secs.unwrap_or(0).max(1) as u32,
            })
        }
        _ => None,
    };

    Some(filters::spec::FilterSpec {
        id,
        name,
        direction,
        action,
        enabled,
        revision: revision.max(0) as u64,
        targets: Targets {
            services_all: service_targets_global,
            service_ids: service_ids.iter().map(|&s| s as u64).collect(),
            agents_all: agent_targets_global,
            agent_ids: agent_ids.into_iter().collect(),
        },
        threshold,
        expires_at_ms: expires_at_ms.unwrap_or(0).max(0) as u64,
        matcher,
    })
}

/// Warm PACKET_FILTERS from the tag_name tables (full unified model).
///
/// Why: filters reached the analyzer ONLY via NATS SettingsUpdate messages,
/// so after a restart mid-game PACKET_FILTERS stayed EMPTY until the
/// backend's 5-minute scheduler fired the next ConfigSync. Services are
/// already warmed from the DB the same way; filters now join them.
///
/// Race safety with updates arriving right after startup:
/// - ConfigSync -> `sync_filters` applies per-id revision gates and removes
///   only stale ids; the warmed set stays deduplicated.
/// - FilterAdd/FilterUpdate (a redelivery the previous instance never
///   acked) -> `add_filter` replaces by id and rejects older revisions.
///
/// Degradation, not crash: on DB failure we log and continue with empty
/// filters - the periodic reconciliation and ConfigSync recover.
async fn warm_filters_from_db(db: &db::DB) {
    let specs = load_filter_specs_from_db(db).await;
    let total = specs.len();
    filters::sync_filters(specs);
    info!("Warmed {} filters from DB into PACKET_FILTERS", total);
}

/// Warm the service cache from the service table. Reused by the startup
/// warm-up and the periodic DB reconciliation. Failure degrades to the
/// previous cache contents (unknown-service drops are counted meanwhile).
async fn warm_services_from_db(db: &db::DB, cache: &cache::Cache) {
    let rows = match sqlx::query_as::<
        _,
        (i64, String, i32, String, bool, bool, bool, bool, bool, bool),
    >(
        "SELECT id, name, port, protocol, chunked_transfer_encoding, decrypt_tls, inflate_websockets, merge_adjacent_packets, urldecode, http_parse FROM service"
    )
    .fetch_all(db.pool())
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!("Failed to load services from DB (keeping previous cache): {}", e);
            return;
        }
    };

    let mut services = cache.services.write().unwrap();
    services.clear();
    for (
        id,
        name,
        port,
        protocol,
        chunked_transfer_encoding,
        decrypt_tls,
        inflate_websockets,
        merge_adjacent_packets,
        urldecode,
        http_parse,
    ) in rows
    {
        // A row whose protocol this build cannot interpret is skipped:
        // traffic for it degrades to counted unknown-service drops
        // instead of being reinterpreted (e.g. UDP as TCP).
        let transport = match protocol.as_str() {
            "TCP" => configuration::updater::dto::TransportProto::Tcp,
            "UDP" => configuration::updater::dto::TransportProto::Udp,
            other => {
                log::warn!(
                    "Skipping service id {} with unknown protocol '{}'",
                    id,
                    other
                );
                continue;
            }
        };
        let Ok(port_u16) = u16::try_from(port) else {
            log::warn!("Skipping service id {} with out-of-range port {}", id, port);
            continue;
        };
        let Ok(id_usize) = usize::try_from(id) else {
            log::warn!("Skipping service id {} overflows usize", id);
            continue;
        };
        services.insert(
            id_usize,
            configuration::updater::dto::Service {
                name,
                port: port_u16,
                protocol: transport,
                chunked_transfer_encoding,
                decrypt_tls,
                inflate_websockets,
                merge_adjacent_packets,
                urldecode,
                http_parse,
            },
        );
    }
    info!("Loaded {} services from DB into cache", services.len());
}

/// Warm the per-agent machine IP registry from agent_config: the
/// effective value is the override when present, else the last reported
/// bootstrap value. Values that no longer parse (e.g. legacy garbage) are
/// skipped: an agent without a resolvable IP has its packets dropped and
/// counted until a snapshot re-registers it, never guessed.
async fn warm_agent_registry_from_db(db: &db::DB, cache: &cache::Cache) {
    let rows = match sqlx::query_as::<_, (String, Option<String>, Option<String>, i64)>(
        "SELECT agent_id, machine_ip_override, reported_machine_ip, last_acked_revision \
         FROM agent_config",
    )
    .fetch_all(db.pool())
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!(
                "Failed to load agent registry from DB (keeping previous registry): {}",
                e
            );
            return;
        }
    };

    let mut registered = 0;
    let row_count = rows.len();
    for (agent_id, override_ip, reported_ip, revision) in rows {
        let value = override_ip
            .or_else(|| reported_ip.clone())
            .unwrap_or_default();
        let Ok(machine_ip) = value.parse() else {
            continue;
        };
        cache.set_agent_network(&agent_id, machine_ip, revision.max(0) as u64);
        registered += 1;
    }
    info!(
        "Loaded {} agent machine IPs from DB into registry ({} rows)",
        registered, row_count
    );
}

/// Bounded interval between reconciliation passes.
const RECONCILIATION_INTERVAL_SECS: u64 = 60;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    debug!("{:?}", config);

    let cache = Arc::new(cache::Cache::new(config.cache_stream_ttl_secs));

    // Periodic eviction task for expired streams and unused locks
    tokio::spawn({
        let cache = cache.clone();
        let interval_secs = config.cache_eviction_interval_secs;
        async move {
            use tokio::time::{interval, Duration};
            let mut cleanup_interval = interval(Duration::from_secs(interval_secs));
            cleanup_interval.tick().await;

            loop {
                cleanup_interval.tick().await;
                let evicted = cache.streams.evict_expired();
                cache.cleanup_unused_locks();
                if evicted > 0 {
                    debug!(
                        "Cache eviction: removed {} expired streams, {} remaining",
                        evicted,
                        cache.streams.len()
                    );
                }
            }
        }
    });

    // Initialize the DB pool before anything else: the startup warm-ups below
    // read from it and must complete before any NATS consumption begins.
    let db = db::DB::init(&config).await.unwrap();

    // Load proto schemas on startup
    load_proto_schemas(&db, &cache).await;

    // Periodic proto reload (every 60s — picks up newly uploaded .proto files)
    tokio::spawn({
        let cache = cache.clone();
        let config = config.clone();
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            interval.tick().await; // skip first tick
            loop {
                interval.tick().await;
                if let Ok(db) = db::DB::init(&config).await {
                    load_proto_schemas(&db, &cache).await;
                }
            }
        }
    });

    // Warm configuration from the DB before consuming packets: services,
    // filters and the per-agent machine IP registry.
    warm_services_from_db(&db, &cache).await;
    warm_agent_registry_from_db(&db, &cache).await;

    // Create the shared WS activity aggregator (flushers record into it,
    // one publisher task sends a single summary frame per second on the
    // WS topic — replaces per-packet NATS publishes, see ws_aggregator.rs).
    let ws_aggregator = ws_aggregator::shared_aggregator();
    if let Ok(ws_nats) = message_broker::create_consumer(&config).await {
        // Answer /api/version probes from the backend (same connection).
        version::spawn_version_responder(ws_nats.clone(), config.nats_info_subject.clone());
        ws_aggregator::spawn_ws_aggregate_publisher(
            ws_nats,
            config.nats_websocket_packets_topic.clone(),
            ws_aggregator.clone(),
        );
    } else {
        // No NATS for WS events: aggregation still runs (cheap), frames
        // just have no publisher — browsers rely on periodic refresh.
        log::warn!(
            "No NATS connection for WS activity publishing — WebSocket live updates disabled"
        );
    }

    // Warm packet filters from DB before the NATS loops start. Until this
    // existed, a restart mid-game left PACKET_FILTERS empty for up to 5
    // minutes (next backend ConfigSync) — silently lost flags.
    warm_filters_from_db(&db).await;

    // Threshold alert/block events: bounded queue drained into
    // filter_event every 2s with the bounded-retry insert pattern.
    filter_events::spawn_event_flusher(db.clone());

    // Threshold counter idle sweep: removes windows for filters that are
    // gone or quiet beyond window+cooldown, keeping the counter map within
    // its capacity bound in steady state.
    tokio::spawn(async {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        interval.tick().await; // skip first tick
        loop {
            interval.tick().await;
            for (_id, window_secs, cooldown_secs) in filters::threshold_configs_snapshot() {
                thresholds::sweep_idle(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    window_secs,
                    cooldown_secs,
                );
            }
        }
    });

    // Supervised update listener: construction and processing failures are
    // contained in a retry loop with bounded backoff. No spawned-task
    // error may permanently kill updates while the packet consumer keeps
    // running with a stale cache; NATS outages degrade to bounded retries.
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let mut backoff_secs: u64 = 1;
            loop {
                let started = std::time::Instant::now();
                let outcome = async {
                    let update_listener =
                        configuration::updater::UpdateListener::new(&config).await?;
                    update_listener
                        .listen_to_updates(&config.nats_updates_topic, cache.clone())
                        .await
                }
                .await;
                match outcome {
                    Ok(()) => {
                        log::warn!("Update listener ended, restarting it");
                    }
                    Err(e) => {
                        log::error!("Update listener failed, retrying: {e:#}");
                    }
                }
                // A listener that ran healthy for a while resets the
                // backoff; repeated fast failures climb to the ceiling.
                if started.elapsed() >= std::time::Duration::from_secs(60) {
                    backoff_secs = 1;
                }
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    });

    // Periodic DB reconciliation: services, filters and the agent registry
    // re-read on a bounded interval, bounding how long a missed update
    // event can stay effective. Never restarts the consumer or the packet
    // path: it only swaps cache contents.
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                RECONCILIATION_INTERVAL_SECS,
            ));
            interval.tick().await; // skip first tick (startup warm-up just ran)
            loop {
                interval.tick().await;
                match db::DB::init(&config).await {
                    Ok(recon_db) => {
                        warm_services_from_db(&recon_db, &cache).await;
                        warm_agent_registry_from_db(&recon_db, &cache).await;
                        warm_filters_from_db(&recon_db).await;
                    }
                    Err(e) => {
                        log::warn!(
                            "Reconciliation skipped (DB unavailable, retrying next interval): {}",
                            e
                        );
                    }
                }
            }
        }
    });

    // Raw packets listener (consumer is only used by the consume loop below)
    let consumer = message_broker::create_consumer(&config).await.unwrap();

    // Spawn the sharded batch flushers: N tasks, each owning the streams
    // whose ids hash to it (per-stream order), bounded queue per task with
    // drop+count on overflow so the NATS consumer never blocks.
    // Starred-segment rotation: a bounded poller keeps starred segments and
    // the segment byte limit in a tracker; the packet path consults it and
    // rotates over-limit favorites into linked continuations. Failures are
    // contained (stale snapshot = a delayed rotation, never lost packets).
    let retention_tracker = Arc::new(retention::RetentionTracker::new());
    retention::spawn_retention_poller(db.clone(), retention_tracker.clone(), 2);

    let flusher_db = db::DB::init(&config).await.unwrap();
    let batch_tx = batch_flusher::spawn_flushers(
        config.batch_flushers,
        config.batch_size * 4,
        flusher_db,
        batch_flusher::FlusherOptions {
            batch_size: config.batch_size,
            flush_interval_ms: config.batch_flush_interval_ms,
        },
        cache.clone(),
        ws_aggregator,
        retention_tracker.clone(),
    );

    match message_broker::consume_raw_packets(
        consumer,
        cache,
        Arc::new(config),
        db,
        Arc::new(batch_tx),
        retention_tracker,
    )
    .await
    {
        Ok(_) => (),
        Err(e) => {
            error!("Error consuming raw packets: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shared analyzer/agent contract fixtures (filter-dsl crate) must
    /// produce identical results here.
    #[test]
    fn shared_analyzer_agent_fixtures_pass() {
        filter_dsl::fixtures::run_fixtures().expect("shared fixtures must pass");
    }

    /// Payload row maps with direction normalization and byte template.
    #[test]
    fn payload_row_maps_to_spec_like_config_sync_dto() {
        let spec = build_spec_from_row(
            7,
            "flag_leak".to_string(),
            "substr".to_string(),
            "flag{".to_string(),
            "in".to_string(),
            "flag".to_string(),
            true,
            None,
            None,
            None,
            true,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            3,
            vec![],
            vec![],
        )
        .expect("payload row must map");
        assert_eq!(spec.id, 7);
        assert_eq!(spec.name, "flag_leak");
        assert!(spec.enabled);
        assert_eq!(spec.action, filters::spec::Action::Flag);
        assert_eq!(spec.revision, 3);
        assert!(spec.targets.services_all && spec.targets.agents_all);
        match spec.matcher {
            filters::spec::MatcherSpec::Payload { kind, payload } => {
                assert_eq!(kind, "substr");
                assert_eq!(payload, b"flag{".to_vec());
            }
            other => panic!("wrong matcher: {:?}", other),
        }
    }

    /// Template travels as raw bytes (multi-byte UTF-8 preserved) and
    /// direction mapping is case-insensitive.
    #[test]
    fn payload_row_direction_is_case_insensitive_and_template_is_bytes() {
        let spec = build_spec_from_row(
            1,
            "n".to_string(),
            "bsubstr".to_string(),
            "флаг".to_string(),
            "OUT".to_string(),
            "tag".to_string(),
            true,
            None,
            None,
            None,
            true,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(
            spec.direction,
            filters::filterDTO::FilterDirection::Outbound
        );
        match spec.matcher {
            filters::spec::MatcherSpec::Payload { payload, .. } => {
                assert_eq!(payload, "флаг".as_bytes().to_vec());
            }
            other => panic!("wrong matcher: {:?}", other),
        }
    }

    /// IP/CIDR row maps onto the normalized network + endpoint.
    #[test]
    fn ip_cidr_row_maps_network_and_endpoint() {
        let spec = build_spec_from_row(
            2,
            "ban".to_string(),
            "ip_cidr".to_string(),
            String::new(),
            "both".to_string(),
            "block".to_string(),
            true,
            Some("10.0.0.5/32".parse().unwrap()),
            Some("src".to_string()),
            None,
            true,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            vec![],
            vec![],
        )
        .unwrap();
        match spec.matcher {
            filters::spec::MatcherSpec::IpCidr {
                network,
                prefix,
                endpoint,
            } => {
                assert_eq!(network, "10.0.0.5".parse::<std::net::IpAddr>().unwrap());
                assert_eq!(prefix, 32);
                assert_eq!(endpoint, filters::spec::IpEndpointSel::Src);
            }
            other => panic!("wrong matcher: {:?}", other),
        }
    }

    /// Expression row carries the validated source; threshold fields map
    /// onto the threshold config; unknown actions reject the row.
    #[test]
    fn expression_threshold_and_unknown_action_rows() {
        let expr = build_spec_from_row(
            3,
            "e".to_string(),
            "expression".to_string(),
            String::new(),
            "both".to_string(),
            "alert".to_string(),
            true,
            None,
            None,
            Some("protocol == \"TCP\"".to_string()),
            true,
            true,
            Some(5),
            Some(10),
            Some("stream".to_string()),
            Some(30),
            Some(60),
            Some(1_700_000_000_000),
            1,
            vec![41],
            vec![],
        )
        .unwrap();
        let threshold = expr.threshold.as_ref().unwrap();
        assert_eq!(threshold.count, 5);
        assert_eq!(threshold.window_secs, 10);
        assert!(matches!(threshold.group_by, filters::spec::GroupBy::Stream));
        assert_eq!(threshold.block_secs, 60);
        assert_eq!(expr.expires_at_ms, 1_700_000_000_000);
        assert_eq!(expr.targets.service_ids, [41u64].into_iter().collect());
        match expr.matcher {
            filters::spec::MatcherSpec::Expression { source } => {
                assert_eq!(source, "protocol == \"TCP\"");
            }
            other => panic!("wrong matcher: {:?}", other),
        }

        assert!(
            build_spec_from_row(
                4,
                "x".to_string(),
                "substr".to_string(),
                "a".to_string(),
                "both".to_string(),
                "not-an-action".to_string(),
                true,
                None,
                None,
                None,
                true,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                vec![],
                vec![],
            )
            .is_none(),
            "unknown action must not map"
        );
    }
}
