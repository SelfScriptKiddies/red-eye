pub mod configuration;
pub mod message_broker;
pub mod filters;
pub mod db;
pub mod ethernet;
pub mod cache;
pub mod process_packet;
pub mod http_parser;
pub mod grpc_parser;
pub mod proto_schema;
pub mod batch_flusher;
pub mod ws_aggregator;
pub mod wire;
use crate::configuration::init;
use crate::filters::add_filter;
use crate::filters::filterDTO::FilterDirection;
use log::{info, debug, error};
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

/// Pure mapping of one tag_name row (as selected by `warm_filters_from_db`)
/// to `add_filter` arguments — extracted so the row semantics are unit-testable
/// without a database.
///
/// Mirrors the backend ConfigSync mapping (backend/src/api/updater.rs,
/// `impl From<tag_dto::TagName> for Filter`, fed by
/// tag_repository::get_all_tag_names):
///   pattern_name   <- tag_name.name
///   filter_type    <- tag_name.template_type
///   filter_payload <- tag_name.template.as_bytes()
///   direction      <- tag_name.direction via FilterDirection::from_tag_direction
fn tag_name_row_to_filter_args(
    row: (i64, String, String, String, String),
) -> (usize, String, String, Vec<u8>, FilterDirection) {
    let (id, name, template_type, template, direction) = row;
    (
        id as usize,
        name,
        template_type,
        template.into_bytes(),
        FilterDirection::from_tag_direction(&direction),
    )
}

/// Warm PACKET_FILTERS from the tag_name table at startup.
///
/// Why: filters reached the analyzer ONLY via NATS SettingsUpdate messages
/// (FilterAdd/FilterUpdate/ConfigSync — see configuration/updater.rs), so
/// after a restart mid-game PACKET_FILTERS stayed EMPTY until the backend's
/// 5-minute scheduler fired the next ConfigSync or an operator edited a
/// tag — up to 5 minutes of silently lost flag detections. Services are
/// already warmed from the DB the same way below; filters now join them.
///
/// The SELECT mirrors the backend's ConfigSync source
/// (tag_repository::get_all_tag_names: `id, name, color, template_type,
/// template, direction, special_type FROM tag_name ORDER BY name`); we
/// fetch exactly the columns the Filter dto consumes. Rows with filter
/// types the analyzer does not implement are skipped by `add_filter` with
/// an error log — same behavior as the ConfigSync path.
///
/// Race safety with updates arriving right after startup (verified in
/// filters.rs tests):
/// - ConfigSync -> `sync_filters` diffs BY ID (adds only missing ids,
///   removes stale ones), so a sync carrying the same rows this warm-up
///   just loaded is a no-op — it cannot duplicate them.
/// - FilterAdd/FilterUpdate (e.g. a JetStream redelivery of a message the
///   previous analyzer instance never acked) -> `add_filter` is idempotent
///   by id (replaces an existing entry), so no double-registration.
///
/// Degradation, not crash: on DB failure we log the error and continue
/// with empty filters — the 5-minute ConfigSync still recovers.
async fn warm_filters_from_db(db: &db::DB) {
    let rows = match sqlx::query_as::<_, (i64, String, String, String, String)>(
        "SELECT id, name, template_type, template, direction FROM tag_name ORDER BY name",
    )
    .fetch_all(db.pool())
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!(
                "Failed to load filters from DB, continuing with empty filters \
                 (5-minute ConfigSync will recover): {}",
                e
            );
            return;
        }
    };

    let total = rows.len();
    let mut registered = 0;
    for row in rows {
        let (id, pattern_name, pattern_type, payload, direction) = tag_name_row_to_filter_args(row);
        if add_filter(id, pattern_name, pattern_type, &payload, direction) {
            registered += 1;
        }
    }
    info!(
        "Loaded {} filters from DB into PACKET_FILTERS ({} tag_name rows skipped, reasons logged above)",
        registered,
        total - registered
    );
}

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    debug!("{:?}", config);

    let cache = Arc::new(
        cache::Cache::new(config.cache_stream_ttl_secs)
    );

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
                    debug!("Cache eviction: removed {} expired streams, {} remaining",
                        evicted, cache.streams.len());
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

    // Warm service cache from DB before consuming packets
    {
        let rows = sqlx::query_as::<_, (i64, String, i32, bool, bool, bool, bool, bool, bool)>(
            "SELECT id, name, port, chunked_transfer_encoding, decrypt_tls, inflate_websockets, merge_adjacent_packets, urldecode, http_parse FROM service"
        )
        .fetch_all(db.pool())
        .await
        .unwrap_or_default();

        let mut services = cache.services.write().unwrap();
        for (id, name, port, chunked_transfer_encoding, decrypt_tls, inflate_websockets, merge_adjacent_packets, urldecode, http_parse) in rows {
            services.insert(id as usize, configuration::updater::dto::Service {
                name,
                port: port as u16,
                chunked_transfer_encoding,
                decrypt_tls,
                inflate_websockets,
                merge_adjacent_packets,
                urldecode,
                http_parse,
            });
        }
        info!("Loaded {} services from DB into cache", services.len());
    }

    // Create the shared WS activity aggregator (flushers record into it,
    // one publisher task sends a single summary frame per second on the
    // WS topic — replaces per-packet NATS publishes, see ws_aggregator.rs).
    let ws_aggregator = ws_aggregator::shared_aggregator();
    if let Ok(ws_nats) = message_broker::create_consumer(&config).await {
        ws_aggregator::spawn_ws_aggregate_publisher(
            ws_nats,
            config.nats_websocket_packets_topic.clone(),
            ws_aggregator.clone(),
        );
    } else {
        // No NATS for WS events: aggregation still runs (cheap), frames
        // just have no publisher — browsers rely on periodic refresh.
        log::warn!("No NATS connection for WS activity publishing — WebSocket live updates disabled");
    }

    // Warm packet filters from DB before the NATS loops start. Until this
    // existed, a restart mid-game left PACKET_FILTERS empty for up to 5
    // minutes (next backend ConfigSync) — silently lost flags.
    warm_filters_from_db(&db).await;

    // Update listener — spawned only AFTER the DB warm-ups above, so no
    // SettingsUpdate (ConfigSync / FilterAdd redelivery) can be applied on
    // top of a partially warmed filter set. Race safety: see the doc
    // comment on warm_filters_from_db and the tests in filters.rs
    // (sync_filters diffs by id; add_filter replaces by id).
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let update_listener = configuration::updater::UpdateListener::new(&config).await;
            update_listener.listen_to_updates(&config.nats_updates_topic, cache).await.unwrap();
        }
    });

    // Raw packets listener (consumer is only used by the consume loop below)
    let consumer = message_broker::create_consumer(&config).await.unwrap();

    // Spawn the sharded batch flushers: N tasks, each owning the streams
    // whose ids hash to it (per-stream order), bounded queue per task with
    // drop+count on overflow so the NATS consumer never blocks.
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
    );

    match message_broker::consume_raw_packets(consumer, cache, Arc::new(config), db, Arc::new(batch_tx)).await {
        Ok(_) => (),
        Err(e) => {
            error!("Error consuming raw packets: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The warm-up must produce exactly the arguments the backend's
    /// ConfigSync would have encoded for the same tag_name row (backend
    /// From<tag_dto::TagName> for Filter).
    #[test]
    fn tag_name_row_maps_to_filter_args_like_config_sync_dto() {
        let (id, name, ftype, payload, direction) = tag_name_row_to_filter_args((
            7,
            "flag_leak".to_string(),
            "substr".to_string(),
            "flag{".to_string(),
            "in".to_string(),
        ));
        assert_eq!(id, 7);
        assert_eq!(name, "flag_leak");
        assert_eq!(ftype, "substr");
        assert_eq!(payload, b"flag{".to_vec());
        assert_eq!(direction, FilterDirection::Inbound);
    }

    /// Template travels as raw bytes (multi-byte UTF-8 preserved) and
    /// direction matching is case-insensitive, mirroring the backend dto.
    #[test]
    fn tag_name_row_direction_is_case_insensitive_and_template_is_bytes() {
        let (_, _, _, payload, direction) = tag_name_row_to_filter_args((
            1,
            "n".to_string(),
            "bsubstr".to_string(),
            "флаг".to_string(),
            "OUT".to_string(),
        ));
        assert_eq!(payload, "флаг".as_bytes().to_vec());
        assert_eq!(direction, FilterDirection::Outbound);
    }
}
