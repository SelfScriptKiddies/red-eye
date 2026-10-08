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
mod health;



// Serialize a batch of packet events once and broadcast as a single frame.
// Wire contract: {"type":"packet_batch","events":[ E1, E2, ... ]} where each E
// is the same per-event JSON object shape sent before batching was introduced.
fn flush_batch(tx: &broadcast::Sender<String>, events: Vec<serde_json::Value>) {
    if events.is_empty() {
        return;
    }
    let count = events.len();
    let batch = serde_json::json!({ "type": "packet_batch", "events": events });
    match serde_json::to_string(&batch) {
        Ok(json) => match tx.send(json) {
            Ok(_) => debug!("Broadcasted packet batch of {} events", count),
            Err(e) => debug!("No WebSocket clients connected or channel full: {}", e),
        },
        Err(e) => error!("Failed to serialize packet batch to JSON: {}", e),
    }
}

fn init() -> ApplicationConfig {
    let config: ApplicationConfig = match ApplicationConfig::parse() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Got error while parsing configuration: {e:?}");
            std::process::exit(1);
        }
    };

    // Session cookie encryption key is mandatory: a known default key would
    // let anyone who reads the repo forge session cookies. Checked before
    // anything else starts (DB pool, NATS, HTTP listener).
    if config.cookie_key.len() < 32 {
        eprintln!(
            "COOKIE_KEY is missing or too short ({} bytes, need at least 32).\n\
             Generate a key and set it in .env (or pass --web-cookie-key):\n\
             COOKIE_KEY=$(openssl rand -base64 32)",
            config.cookie_key.len()
        );
        std::process::exit(1);
    }

    // Possible values: trace|debug|info|warn|error
    let logging_level = match config.logging_level.to_lowercase().as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => {
            eprintln!(
                "Unknown LOGGING_LEVEL '{}', falling back to info",
                config.logging_level
            );
            log::LevelFilter::Info
        }
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

    health::init_start_time();
    let health_monitor = Arc::new(health::HealthMonitor::new(
        config.health_query_warn_ms,
        config.health_query_critical_ms,
    ));
    let health_data = web::Data::new(health_monitor.clone());

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

    // Materialized view refresh via LISTEN/NOTIFY
    {
        let refresh_pool = pool.clone();
        tokio::spawn(async move {
            use tokio::time::{sleep, Duration, Instant};

            let mut listener = sqlx::postgres::PgListener::connect_with(&refresh_pool).await.unwrap();
            listener.listen("refresh_streams").await.unwrap();
            info!("Listening for refresh_streams notifications");

            let debounce = Duration::from_secs(5);
            let mut last_refresh = Instant::now() - debounce;

            loop {
                match listener.recv().await {
                    Ok(_) => {
                        let since_last = last_refresh.elapsed();
                        if since_last < debounce {
                            sleep(debounce - since_last).await;
                        }
                        match sqlx::query("SELECT refresh_streams_view()")
                            .execute(&refresh_pool)
                            .await
                        {
                            Ok(_) => {
                                debug!("Refreshed streams_list_view");
                                last_refresh = Instant::now();
                            }
                            Err(e) => error!("Failed to refresh mat view: {}", e),
                        }
                    }
                    Err(e) => {
                        error!("LISTEN error: {}", e);
                        sleep(Duration::from_secs(1)).await;
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
    spawn_heartbeat_listener(agent_nats_client.clone(), agent_service.clone(), pool.clone());

    // Create broadcaster for WebSocket connections
    let (tx, _) = broadcast::channel::<String>(1000);
    let broadcaster_tx = Arc::new(tx);
    let broadcaster = web::Data::new(broadcaster_tx.clone());

    // Spawn special trigger handler (flag farm + alert notifications)
    api::special_triggers::spawn_special_trigger_handler(
        pool.clone(),
        broadcaster_tx.clone(),
    );

    // Subscribe to NATS events and broadcast to WebSocket clients.
    // Reuse the already-connected agent client: a client connected inside this
    // spawned task never delivers messages under the actix runtime.
    let broadcaster_tx_clone = broadcaster_tx.clone();
    let ws_nats_client = agent_nats_client.clone();
    let ws_topic = config.nats_websocket_topic.clone();
    tokio::spawn(async move {
        info!("Subscribing to NATS topic {} for WebSocket events", ws_topic);

        let mut subscriber = match ws_nats_client.subscribe(ws_topic.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to NATS topic {}: {}", ws_topic, e);
                return;
            }
        };

        // Decode packet event (same structure as in traffic-analyzer)
        #[derive(serde::Serialize, serde::Deserialize, bincode::Decode)]
        struct PacketEvent {
            packet_id: i32,
            stream_id: usize,
            timestamp_seconds: u64,
            timestamp_microseconds: u64,
            direction: String,
        }

        // Coalesce incoming packet events into batches to avoid one serialization +
        // broadcast + socket write per packet at high packet rates. Flush at most
        // every 100ms or once 100 events accumulate, whichever comes first.
        use tokio::time::{sleep_until, Duration, Instant};
        let mut buffer: Vec<serde_json::Value> = Vec::new();
        let mut flush_deadline: Option<Instant> = None;

        loop {
            // When no batch is pending, the timer branch must never fire; a
            // pending() future keeps it disabled without unwrapping the deadline.
            let flush_timer = async {
                match flush_deadline {
                    Some(d) => sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                maybe_msg = subscriber.next() => {
                    let Some(message) = maybe_msg else { break };
                    let payload = message.payload.to_vec();
                    match serde_json::from_slice::<PacketEvent>(&payload) {
                        Ok(event) => match serde_json::to_value(&event) {
                            Ok(v) => {
                                if buffer.is_empty() {
                                    flush_deadline = Some(Instant::now() + Duration::from_millis(100));
                                }
                                buffer.push(v);
                                if buffer.len() >= 100 {
                                    flush_batch(&broadcaster_tx_clone, std::mem::take(&mut buffer));
                                    flush_deadline = None;
                                }
                            }
                            Err(e) => error!("Failed to serialize packet event to JSON: {}", e),
                        },
                        Err(e) => error!("Failed to decode packet event from NATS: {}", e),
                    }
                }
                _ = flush_timer => {
                    flush_batch(&broadcaster_tx_clone, std::mem::take(&mut buffer));
                    flush_deadline = None;
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
            .app_data(health_data.clone())
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
