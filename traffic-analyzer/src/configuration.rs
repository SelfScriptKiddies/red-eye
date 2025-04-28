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
        default = "\"db\".to_string()")
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
        default = "\"user\".to_string()")
    ]
    pub db_username: String,
    #[source(
        clap(long="db-password", short='p', help="Database password", help_heading = "Database"),
        env = "DB_PASSWORD",
        config = "database.password",
        default = "\"user\".to_string()")
    ]
    pub db_password: String,
    #[source(
        clap(long="db-schema", short='s', help="Database schema", help_heading = "Database"),
        env = "DB_SCHEMA",
        config = "database.schema",
        default = "\"red-eye\".to_string()")
    ]
    pub db_schema: String,

    /// Kafka config
    #[source(
        clap(long="kafka-host", short='b', help="Kafka broker host", help_heading = "Kafka"),
            env = "KAFKA_HOST",
            config = "kafka.host",
            default = "\"kafka\".to_string()")
        ]
    pub kafka_broker: String,
    #[source(
        clap(long="kafka-raw-packets-topic", short='R', help="Kafka broker topic", help_heading = "Kafka"),
        env = "KAFKA_RAW_PACKETS_TOPIC",
        config = "kafka.raw_packets_topic",
        default = "\"raw_packets\".to_string()")
    ]
    pub kafka_raw_packets_topic: String,
    #[source(
        clap(long="kafka-websocket-packets-topic", short='W', help="Kafka broker topic", help_heading = "Kafka"),
        env = "KAFKA_WS_TOPIC",
        config = "kafka.websocket_packets_topic",
        default = "\"websocket_packets\".to_string()")
    ]
    pub kafka_websocket_packets_topic: String,
    #[source(
        clap(long="kafka-updates-topic", short='U', help="Kafka broker topic", help_heading = "Kafka"),
        env = "KAFKA_UPDATES_TOPIC",
        config = "kafka.updates_topic",
        default = "\"updates\".to_string()")
    ]
    pub kafka_updates_topic: String,

    /// Service UUID
    #[source(
        clap(long="service-uuid", help="Service UUID", help_heading = "Service options"),
        env = "SERVICE_UUID",
        config = "service.uuid",
        default = "uuid::Uuid::new_v4().to_string()"
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
        clap(long="cache-streams-ttl-seconds", help="TTL for cached streams in seconds (0 means no TTL)", help_heading = "Cache"),
        env = "CACHE_STREAMS_TTL_SECONDS",
        config = "cache.streams_ttl_seconds",
        default = 600
    )]
    pub cache_streams_ttl_seconds: u64,

    /// Logging configuration
    #[source(
        clap(short='l', long="logging-level", help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "\"trace\".to_string()"
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
        _ => log::LevelFilter::Trace,
    };

    let mut clog = colog::default_builder();
    clog.filter(None, logging_level);
    clog.init();

    config
}