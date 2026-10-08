pub mod updater;

use config_manager::{config, ConfigInit};
use std::fmt::Debug;


#[derive(Debug, Clone)]
#[config(
    clap(version, author),
    file(
        format = "toml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "CONFIG_FILE",
        optional
    )
)]
pub struct ApplicationConfig {
    /// Database config
    #[source(
        clap(long="db-host", short='H', help="Database domain", help_heading = "Database"),
        env = "DB_HOST",
        config = "database.host",
        default = "db")
    ]
    pub db_host: String,
    #[source(
        clap(long="db-port", short='P', help="Database port", help_heading = "Database"),
        env = "DB_PORT",
        config = "database.port",
        default = 5432)
    ]
    pub db_port: u16,
    #[source(
        clap(long="db-username", short='u', help="Database username", help_heading = "Database"),
        env = "DB_USERNAME",
        config = "database.username",
        default = "red-eye-user")
    ]
    pub db_username: String,
    #[source(
        clap(long="db-password", short='p', help="Database password", help_heading = "Database"),
        env = "DB_PASSWORD",
        config = "database.password",
        default = "7b375636c1c9ba28")
    ]
    pub db_password: String,
    #[source(
        clap(long="db-schema", short='s', help="Database schema", help_heading = "Database"),
        env = "DB_SCHEMA",
        config = "database.schema",
        default = "red-eye")
    ]
    pub db_schema: String,

    /// Kafka config
    #[source(
        clap(long="nats-host", short='b', help="NATS broker host", help_heading = "NATS"),
        env = "NATS_HOST",
        config = "nats.host",
        default = "nats")
    ]
    pub nats_host: String,
    #[source(
        clap(long="nats-login", help="NATS login", help_heading = "NATS"),
        env = "NATS_LOGIN",
        config = "nats.login",
        default = "user")
    ]
    pub nats_login: String,
    #[source(
        clap(long="nats-password", help="NATS password", help_heading = "NATS"),
        env = "NATS_PASSWORD",
        config = "nats.password",
        default = "password")
    ]
    pub nats_password: String,
    #[source(
        clap(long="nats-raw-packets-topic", short='R', help="NATS broker topic", help_heading = "NATS"),
        env = "NATS_RAW_PACKETS_TOPIC",
        config = "nats.raw_packets_topic",
        default = "raw_packets")
    ]
    pub nats_raw_packets_topic: String,
    #[source(
        clap(long="nats-websocket-packets-topic", short='W', help="NATS broker topic", help_heading = "NATS"),
        env = "NATS_WS_TOPIC",
        config = "nats.websocket_packets_topic",
        default = "websocket_packets")
    ]
    pub nats_websocket_packets_topic: String,
    #[source(
        clap(long="nats-updates-topic", short='U', help="NATS broker topic", help_heading = "NATS"),
        env = "NATS_UPDATES_TOPIC",
        config = "nats.updates_topic",
        default = "updates")
    ]
    pub nats_updates_topic: String,

    /// Service UUID
    #[source(
        clap(long="service-uuid", help="Service UUID", long_help="UUID for registering in kafka - useful for debugging, mostly stays with default value", help_heading = "Service options"),
        env = "SERVICE_UUID",
        config = "service.uuid",
        default = uuid::Uuid::new_v4().to_string()
    )]
    pub service_uuid: String,

    /// Cache configuration
    #[source(
        clap(long="cache-streams-capacity", help="LRU cache capacity for streams", help_heading = "Cache"),
        env = "CACHE_STREAMS_CAPACITY",
        config = "cache.streams_capacity",
        default = 1000
    )]
    pub cache_streams_capacity: usize,

    #[source(
        clap(long="cache-stream-ttl-secs", help="Evict cached streams not seen for this many seconds", help_heading = "Cache"),
        env = "CACHE_STREAM_TTL_SECS",
        config = "cache.stream_ttl_secs",
        default = 120
    )]
    pub cache_stream_ttl_secs: u64,

    #[source(
        clap(long="cache-eviction-interval-secs", help="How often to sweep expired cache entries", help_heading = "Cache"),
        env = "CACHE_EVICTION_INTERVAL_SECS",
        config = "cache.eviction_interval_secs",
        default = 30
    )]
    pub cache_eviction_interval_secs: u64,

    #[source(
        clap(long="batch-size", help="Max packets per batch insert", help_heading = "Batch"),
        env = "BATCH_SIZE",
        config = "batch.size",
        default = 500
    )]
    pub batch_size: usize,

    #[source(
        clap(long="batch-flush-interval-ms", help="Max ms before flushing a partial batch", help_heading = "Batch"),
        env = "BATCH_FLUSH_INTERVAL_MS",
        config = "batch.flush_interval_ms",
        default = 100
    )]
    pub batch_flush_interval_ms: u64,

    #[source(
        clap(long="analyzer-concurrency", help="Max concurrent packet processing tasks", help_heading = "Performance"),
        env = "ANALYZER_CONCURRENCY",
        config = "performance.concurrency",
        default = 256
    )]
    pub analyzer_concurrency: usize,

    /// Sniffer configuration
    #[source(
        clap(short='i', long="machine-ip", help="Machine IP address", help_heading = "Sniffer"),
        env = "MACHINE_IP",
        config = "sniffer.machine_ip",
        default = "0.0.0.0"
    )]
    pub machine_ip: String,

    /// Logging configuration
    #[source(
        clap(short='l', long="logging-level", help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "info"
    )]
    pub logging_level: String,
}

pub fn init() -> ApplicationConfig {
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