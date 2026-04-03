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
        cache::Cache::new(config.cache_streams_capacity)
    );

    // Periodic cleanup task for unused stream locks (every 5 minutes)
    // This prevents memory accumulation from old stream locks
    tokio::spawn({
        let cache = cache.clone();
        async move {
            use tokio::time::{interval, Duration};
            let mut cleanup_interval = interval(Duration::from_secs(5 * 60));
            cleanup_interval.tick().await;

            loop {
                cleanup_interval.tick().await;
                cache.cleanup_unused_locks().await;
                debug!("Cleaned up unused stream locks");
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

    match message_broker::consume_raw_packets(consumer, cache, Arc::new(config), db).await {
        Ok(_) => (),
        Err(e) => {
            error!("Error consuming raw packets: {}", e);
        }
    }
}
