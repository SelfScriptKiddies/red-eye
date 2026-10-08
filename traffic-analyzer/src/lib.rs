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
use crate::configuration::init;
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

    // Update listener
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let update_listener = configuration::updater::UpdateListener::new(&config).await;
            update_listener.listen_to_updates(&config.nats_updates_topic, cache).await.unwrap();
        }
    });

    // Raw packets listener
    let consumer = message_broker::create_consumer(&config).await.unwrap();
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

    // Create batch channel and spawn the flusher
    let (batch_tx, batch_rx) = batch_flusher::create_batch_channel(config.batch_size * 4);

    let flusher_db = db::DB::init(&config).await.unwrap();
    let flusher_nats = message_broker::create_consumer(&config).await.ok();
    let flusher_cache = cache.clone();
    let batch_size = config.batch_size;
    let flush_interval = config.batch_flush_interval_ms;
    let nats_ws_topic = config.nats_websocket_packets_topic.clone();
    tokio::spawn(async move {
        batch_flusher::run_batch_flusher(
            batch_rx, flusher_db, batch_size, flush_interval,
            flusher_nats, nats_ws_topic, flusher_cache,
        ).await;
    });

    match message_broker::consume_raw_packets(consumer, cache, Arc::new(config), db, batch_tx).await {
        Ok(_) => (),
        Err(e) => {
            error!("Error consuming raw packets: {}", e);
        }
    }
}
