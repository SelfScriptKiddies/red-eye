use crate::configuration::ApplicationConfig;
use crate::db::establish_connection;
use actix_identity::IdentityMiddleware;
use actix_session::config::CookieContentSecurity;
use actix_session::storage::CookieSessionStore;
use actix_session::SessionMiddleware;
use actix_web::cookie::{Key, SameSite};
use actix_web::middleware::{ErrorHandlers, Logger};
use actix_web::{App, HttpServer, web};
use actix_web::http::StatusCode;
use config_manager::ConfigInit;
use log::{debug, info, error};
use api::auth;
use crate::api::{
    packet::packet_service::PacketService,
    stream::stream_service::StreamService,
    tag::tag_service::TagService,
    service::service_service::ServiceService,
    updater::{UpdateAgent, start_config_sync_scheduler},
    agent::agent_service::{AgentService, spawn_heartbeat_listener},
    l7::l7_service::L7Service,
};
use std::sync::Arc;
use async_nats::ConnectOptions;
use futures_util::StreamExt;
use bincode::config;
use tokio::sync::broadcast;


mod api;
mod configuration;
mod db;



fn init() -> ApplicationConfig {
    let config: ApplicationConfig = match ApplicationConfig::parse() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Got error while parsing configuration: {e:?}");
            std::process::exit(1);
        }
    };

    // Possible values: trace|debug|info|warn|error
    let logging_level = match config.logging_level.to_lowercase().as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Trace,
    };

    let mut clog = colog::default_builder();
    clog.filter(None, logging_level);
    clog.init();

    config
}

pub async fn run() -> std::io::Result<()> {
    let config = init();
    let configuration_data = web::Data::new(config.clone());
    let pool = establish_connection(&config).await;
    let packet_service = web::Data::new(PacketService::new_from_pgpool(pool.clone()));
    let stream_service = web::Data::new(StreamService::new_from_pgpool(pool.clone()));
    let tag_service = web::Data::new(TagService::new_from_pgpool(pool.clone()));
    let service_service = web::Data::new(ServiceService::new_from_pgpool(pool.clone()));
    let l7_service = web::Data::new(L7Service::new_from_pgpool(pool.clone()));
    let pg_pool = web::Data::new(pool.clone());
    
    // Создаём UpdateAgent для использования в web и фоновой задаче
    let updater_arc = Arc::new(UpdateAgent::new(&config).await);
    let updater = web::Data::from(updater_arc.clone());
    
    debug!("{:?}", &config);

    // Запускаем периодическую синхронизацию конфигурации каждые 5 минут
    let service_service_arc = Arc::new(ServiceService::new_from_pgpool(pool.clone()));
    let tag_service_arc = Arc::new(TagService::new_from_pgpool(pool.clone()));
    
    tokio::spawn(async move {
        start_config_sync_scheduler(updater_arc, service_service_arc, tag_service_arc).await;
    });

    // Partition management + cleanup via partition drops
    {
        let cleanup_pool = pool.clone();
        let age_minutes = config.cleanup_stream_age_minutes;
        let interval_minutes = config.cleanup_interval_minutes;
        let max_disk_mb = config.cleanup_max_disk_mb;
        tokio::spawn(async move {
            use tokio::time::{interval, Duration};
            let mut ticker = interval(Duration::from_secs(interval_minutes * 60));
            loop {
                ticker.tick().await;

                // Ensure partitions exist for upcoming data
                match sqlx::query_scalar::<_, i32>(
                    "SELECT ensure_current_partitions(60, 120, 300)"
                )
                .fetch_one(&cleanup_pool)
                .await
                {
                    Ok(created) => {
                        if created > 0 {
                            debug!("Created {} new partitions", created);
                        }
                    }
                    Err(e) => error!("Failed to create partitions: {}", e),
                }

                // Drop old partitions + orphaned streams
                match sqlx::query_as::<_, (i32, i64)>(
                    "SELECT * FROM cleanup_old_data($1)"
                )
                .bind(age_minutes as i32)
                .fetch_one(&cleanup_pool)
                .await
                {
                    Ok((partitions, streams)) => {
                        if partitions > 0 || streams > 0 {
                            info!(
                                "Cleanup: dropped {} partitions, removed {} streams",
                                partitions, streams
                            );
                        }
                    }
                    Err(e) => error!("Cleanup failed: {}", e),
                }

                // Disk pressure check: drop oldest partitions if total exceeds max_disk_mb
                if max_disk_mb > 0 {
                    loop {
                        let total_mb: Option<i64> = sqlx::query_scalar(
                            "SELECT (sum(pg_total_relation_size(inhrelid::regclass)) / (1024*1024))::bigint
                             FROM pg_inherits
                             WHERE inhparent IN ('packets'::regclass, 'tag'::regclass,
                                                  'http_message'::regclass, 'grpc_message'::regclass)"
                        )
                        .fetch_one(&cleanup_pool)
                        .await
                        .ok();

                        match total_mb {
                            Some(mb) if mb > max_disk_mb => {
                                info!("Disk pressure: {}MB > {}MB limit, dropping oldest partitions", mb, max_disk_mb);
                                // Drop the single oldest partition from each table
                                let _ = sqlx::query("SELECT cleanup_old_data(0)")
                                    .execute(&cleanup_pool)
                                    .await;
                            }
                            _ => break,
                        }
                    }
                }
            }
        });
    }

    // Create AgentService for agent management
    let agent_nats_client = ConnectOptions::with_user_and_password(
        config.nats_login.clone(),
        config.nats_password.clone()
    )
    .connect(&config.nats_broker)
    .await
    .expect("Failed to connect to NATS for agent service");

    let agent_service = Arc::new(AgentService::new(agent_nats_client.clone()));
    let agent_service_data = web::Data::new(agent_service.clone());

    // Subscribe to agent heartbeats (syncs block filters on new agent connect)
    spawn_heartbeat_listener(agent_nats_client, agent_service.clone(), pool.clone());

    // Create broadcaster for WebSocket connections
    let (tx, _) = broadcast::channel::<String>(1000);
    let broadcaster_tx = Arc::new(tx);
    let broadcaster = web::Data::new(broadcaster_tx.clone());

    // Spawn special trigger handler (flag farm + alert notifications)
    api::special_triggers::spawn_special_trigger_handler(
        pool.clone(),
        broadcaster_tx.clone(),
    );

    // Subscribe to NATS events and broadcast to WebSocket clients
    let broadcaster_tx_clone = broadcaster_tx.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let nats_client = match ConnectOptions::with_user_and_password(
            config_clone.nats_login.clone(),
            config_clone.nats_password.clone()
        )
        .connect(&config_clone.nats_broker)
        .await
        {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to connect to NATS for WebSocket events: {}", e);
                return;
            }
        };

        let topic = config_clone.nats_websocket_topic.clone();
        info!("Subscribing to NATS topic {} for WebSocket events", topic);
        
        let mut subscriber = match nats_client.subscribe(topic.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to NATS topic {}: {}", topic, e);
                return;
            }
        };

        while let Some(message) = subscriber.next().await {
            let payload = message.payload.to_vec();
            
            // Decode packet event (same structure as in traffic-analyzer)
            #[derive(serde::Serialize, serde::Deserialize, bincode::Decode)]
            struct PacketEvent {
                packet_id: i32,
                stream_id: usize,
                timestamp_seconds: u64,
                timestamp_microseconds: u64,
                direction: String,
            }

            match bincode::decode_from_slice::<PacketEvent, _>(&payload, config::standard()) {
                Ok((event, _)) => {
                    // Convert to JSON for frontend
                    match serde_json::to_string(&event) {
                        Ok(json) => {
                            // Broadcast to all WebSocket clients
                            match broadcaster_tx_clone.send(json.clone()) {
                                Ok(_) => {
                                    debug!("Broadcasted packet event: packet_id={}, stream_id={}", event.packet_id, event.stream_id);
                                }
                                Err(e) => {
                                    debug!("No WebSocket clients connected or channel full: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize packet event to JSON: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to decode packet event from NATS: {}", e);
                }
            }
        }
    });

    HttpServer::new(move || {
        App::new()
            .app_data(pg_pool.clone())
            .app_data(configuration_data.clone())
            .app_data(packet_service.clone())
            .app_data(stream_service.clone())
            .app_data(tag_service.clone())
            .app_data(service_service.clone())
            .app_data(l7_service.clone())
            .app_data(updater.clone())
            .app_data(broadcaster.clone())
            .app_data(agent_service_data.clone())
            .wrap(ErrorHandlers::new().handler(StatusCode::UNAUTHORIZED, auth::add_error_body))
            .wrap(Logger::default())
            .wrap(IdentityMiddleware::default())
            .wrap(
                SessionMiddleware::builder(
                    CookieSessionStore::default(),
                    Key::from(config.cookie_key.clone().as_bytes()),
                )
                .cookie_secure(false)
                .cookie_http_only(false)
                .cookie_same_site(SameSite::Lax)
                .cookie_content_security(CookieContentSecurity::Private)
                .build()
            )
            .configure(auth::auth_config)
            .configure(api::api_config)
    })
    .bind((config.web_host, config.web_port))?
    .run()
    .await
}
