pub mod updater;

use config_manager::{config, ConfigInit};


#[derive(Debug, Clone)]
#[config(
    clap(version, author),
    file(
        format = "toml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "CONFIG_FILE",
        optional=true
    )
)]
pub struct ApplicationConfig {
    /// Database config
    #[source(
        clap(long, short='H', help="Database domain"),
        env = "DB_HOST",
        config = "database.host",
        default = "\"db\".to_string()")
    ]
    pub db_host: String,
    #[source(
        clap(long, short='P', help="Database port"),
        env = "DB_PORT",
        config = "database.port",
        default = 5432)
    ]
    pub db_port: u16,
    #[source(
        clap(long, short='u', help="Database username"),
        env = "DB_USERNAME",
        config = "database.username",
        default = "\"user\".to_string()")
    ]
    pub db_username: String,
    #[source(
        clap(long, short='p', help="Database password"),
        env = "DB_PASSWORD",
        config = "database.password",
        default = "\"user\".to_string()")
    ]
    pub db_password: String,
    #[source(
        clap(long, short='s', help="Database schema"),
        env = "DB_SCHEMA",
        config = "database.schema",
        default = "\"red-eye\".to_string()")
    ]
    pub db_schema: String,

    /// Kafka config
    #[source(
            clap(long, short='b', help="Kafka broker host"),
            env = "KAFKA_HOST",
            config = "kafka.host",
            default = "\"kafka\".to_string()")
        ]
    pub kafka_broker: String,
    #[source(
        clap(long, short='t', help="Kafka broker topic"),
        env = "KAFKA_RAW_PACKETS_TOPIC",
        config = "kafka.raw_packets_topic",
        default = "\"raw_packets\".to_string()")
    ]
    pub kafka_raw_packets_topic: String,
    #[source(
        clap(long, short='w', help="Kafka broker topic"),
        env = "KAFKA_WS_TOPIC",
        config = "kafka.websocket_packets_topic",
        default = "\"websocket_packets\".to_string()")
    ]
    pub kafka_websocket_packets_topic: String,
    #[source(
        clap(long, short='u', help="Kafka broker topic"),
        env = "KAFKA_UPDATES_TOPIC",
        config = "kafka.updates_topic",
        default = "\"updates\".to_string()")
    ]
    pub kafka_updates_topic: String,

    /// Service UUID
    #[source(
        clap(long, help="Service UUID"),
        env = "SERVICE_UUID",
        config = "service.uuid",
        default = "uuid::Uuid::new_v4().to_string()"
    )]
    pub service_uuid: String,

    /// Logging configuration
    #[source(
        clap(short, long, help="Logging level (trace|debug|info|warn|error)"),
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