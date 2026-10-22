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
    agent::agent_service::{AgentService, spawn_docker_containers_listener, spawn_heartbeat_listener},
    l7::l7_service::L7Service,
};
use std::sync::Arc;
use async_nats::ConnectOptions;
use tokio::sync::broadcast;


mod api;
mod configuration;
mod db;
mod db_watchdog;
mod health;
mod wire;
mod ws_activity;



/// Ensure DEFAULT partitions exist for all partitioned tables (idempotent).
/// Inlined SQL twin of ensure_default_partitions() from
/// database/init/06_partitions.sql: the DB-side function only exists after a
/// fresh init, so this runtime path must not depend on it. Pure catalog
/// reads when all DEFAULT partitions already exist.
async fn ensure_default_partitions(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DO $$
         DECLARE t text;
         BEGIN
             FOREACH t IN ARRAY ARRAY['packets','tag','http_message','grpc_message'] LOOP
                 IF NOT EXISTS (
                     SELECT 1 FROM pg_inherits i
                     JOIN pg_class c ON c.oid = i.inhrelid
                     WHERE i.inhparent = t::regclass
                       AND c.relname = t || '_default'
                 ) THEN
                     EXECUTE format('CREATE TABLE %I PARTITION OF %I DEFAULT',
                                    t || '_default', t);
                 END IF;
             END LOOP;
         END $$;",
    )
    .execute(pool)
    .await?;
    Ok(())
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
    // Key::from (cookie crate) slices a 64-byte combined signing+encryption
    // key; anything shorter panics inside actix at worker startup.
    if config.cookie_key.len() < 64 {
        eprintln!(
            "COOKIE_KEY is missing or too short ({} bytes, need at least 64).\n\
             Generate a key and set it in .env (or pass --web-cookie-key):\n\
             COOKIE_KEY=$(openssl rand -base64 64)",
            config.cookie_key.len()
        );
        std::process::exit(1);
    }

    // Web login password is mandatory: the old hardcoded default was a real
    // credential committed to the repo.
    if config.web_password.is_empty() {
        eprintln!(
            "WEB_PASSWORD is not set.\n\
             Set the web login password in .env (or pass --web-password):\n\
             WEB_PASSWORD=<your web login password>"
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

    // Stuck-query watchdog: cancels queries active beyond
    // HEALTH_KILL_QUERY_AFTER_MS and counts them in /api/health.
    db_watchdog::spawn(&config, health_monitor.clone());

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
            use tokio::time::{interval, sleep, Duration};
            let mut ticker = interval(Duration::from_secs(interval_minutes * 60));

            // Partitioned tables managed here (kept in sync with
            // database/init/06_partitions.sql).
            const PARTITIONED_TABLES: [&str; 4] =
                ["packets", "tag", "http_message", "grpc_message"];

            loop {
                ticker.tick().await;

                // Ensure partitions exist for upcoming data.
                // ensure_current_partitions() warns (does not fail) when a
                // range partition cannot be created because rows for that
                // range already sit in DEFAULT — those rows keep landing in
                // DEFAULT until an operator resolves the overlap.
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

                // Runtime DEFAULT-partition ensure (idempotent, live-safe:
                // inlined here because the DB-side function only exists
                // after a fresh init; the runtime path must not depend on
                // it). DEFAULT partitions make out-of-range timestamps
                // (clock skew, long backend outage) degrade into misfiled
                // rows instead of failed INSERTs that drop whole ingest
                // batches. Pure catalog reads when all defaults exist.
                if let Err(e) = ensure_default_partitions(&cleanup_pool).await {
                    error!("Failed to ensure DEFAULT partitions: {}", e);
                }

                // Drop old partitions + prune DEFAULT partitions + orphaned streams
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

                // Disk pressure check: free space WITHOUT ever taking the
                // write target away from the ingest writer:
                //   1. drop only partitions strictly older than the CURRENT
                //      one (floor computed in Rust so protection also holds
                //      against a DB still running the pre-protection
                //      drop_old_partitions / cleanup_old_data);
                //   2. prune old skewed rows from the DEFAULT partitions;
                //   3. trim OLD rows (older than KEEP_WINDOW_SECS, well
                //      above the flush buffer window) from the remaining
                //      current window in bounded 10k chunks.
                // Even at max pressure the current + future partitions and
                // the last few minutes of rows always stay. The old
                // cleanup_old_data(0) call is gone on purpose: it could drop
                // the partition receiving current writes.
                if max_disk_mb > 0 {
                    // Must match the partition interval used by
                    // ensure_current_partitions(60, 120, 300) above.
                    const PARTITION_INTERVAL_SECS: i64 = 300;
                    // Rows newer than this are never deleted under pressure
                    // (flush buffer headroom; batches hold at most seconds
                    // of packets).
                    const KEEP_WINDOW_SECS: i64 = 300;
                    const DELETE_CHUNK: i64 = 10_000;

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

                                let now_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs() as i64)
                                    .unwrap_or(0);
                                // Start of the CURRENT partition: this and
                                // every later partition are untouchable.
                                let protected_from =
                                    (now_ts / PARTITION_INTERVAL_SECS) * PARTITION_INTERVAL_SECS;

                                for table in PARTITIONED_TABLES {
                                    // (1) clamped partition drops
                                    let _ = sqlx::query(
                                        "SELECT drop_old_partitions($1, $2)"
                                    )
                                    .bind(table)
                                    .bind(protected_from)
                                    .execute(&cleanup_pool)
                                    .await;

                                    // (2) old skewed rows in DEFAULT
                                    // (exists by now: ensured earlier this
                                    // tick; table name is from the const
                                    // array, not user input)
                                    let _ = sqlx::query(&format!(
                                        "DELETE FROM {table}_default WHERE timestamp < $1"
                                    ))
                                    .bind(protected_from)
                                    .execute(&cleanup_pool)
                                    .await;
                                }

                                // (3) trim old rows from the current write
                                // window in bounded chunks; keep the fresh
                                // landing zone intact.
                                let trim_before = now_ts - KEEP_WINDOW_SECS;
                                for table in PARTITIONED_TABLES {
                                    loop {
                                        let chunk_sql = format!(
                                            "DELETE FROM {table} WHERE ctid IN \
                                             (SELECT ctid FROM {table} WHERE timestamp < $1 LIMIT {DELETE_CHUNK})"
                                        );
                                        match sqlx::query(&chunk_sql)
                                            .bind(trim_before)
                                            .execute(&cleanup_pool)
                                            .await
                                        {
                                            Ok(res) if res.rows_affected() >= DELETE_CHUNK as u64 => {
                                                // full chunk deleted — pause to
                                                // stay friendly to the writer
                                                sleep(Duration::from_millis(50)).await;
                                            }
                                            Ok(_) => break,
                                            Err(e) => {
                                                error!("Pressure trim of {} failed: {}", table, e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                }
            }
        });
    }

    // NOTE: the old streams_list_view materialized view refresh via
    // LISTEN/NOTIFY ('refresh_streams' channel) was removed together with
    // the matview itself: the streams list now reads stream LEFT JOIN
    // aggregated stream_tag, maintained incrementally by the
    // traffic-analyzer batch flusher. Nothing needs a refresh signal.

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

    // Subscribe to agent Docker container listings (UI "add by container")
    spawn_docker_containers_listener(agent_nats_client.clone(), agent_service.clone());

    // Create broadcaster for WebSocket connections
    let (tx, _) = broadcast::channel::<String>(1000);
    let broadcaster_tx = Arc::new(tx);
    let broadcaster = web::Data::new(broadcaster_tx.clone());

    // Spawn special trigger handler (flag farm + alert notifications)
    api::special_triggers::spawn_special_trigger_handler(
        pool.clone(),
        broadcaster_tx.clone(),
    );

    // Subscribe to NATS per-packet events and broadcast ONE aggregated
    // "activity" frame per second to WebSocket clients (see ws_activity.rs
    // for the wire format and the aggregation semantics).
    // Reuse the already-connected agent client: a client connected inside this
    // spawned task never delivers messages under the actix runtime.
    ws_activity::spawn_ws_bridge(
        agent_nats_client.clone(),
        broadcaster_tx.clone(),
        config.nats_websocket_topic.clone(),
    );

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
