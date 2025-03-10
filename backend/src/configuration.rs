use config_manager::config;

#[derive(Debug, Clone)]
#[config(
    clap(version, author),
    file(
        format = "toml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "demo_config",
        optional=true
    )
)]
pub struct ApplicationConfig {
    /// Web-application config
    #[source(
        clap(long, help="Backend web ip", help_heading = "Web-application config"),
        env = "WEB_IP",
        config = "web.ip",
        default = "\"0.0.0.0\".to_string()"
    )]
    pub web_host: String,
    #[source(
        clap(long, help="Backend web port", help_heading = "Web-application config"),
        env = "WEB_PORT",
        config = "web.port",
        default = 8080
    )]
    pub web_port: u16,
    #[source(
        clap(long, help="Backend web password for login", help_heading = "Web-application config"),
        env = "WEB_PASSWORD",
        config = "web.password",
        default = "\"fbfd83ebcabfd05ace9c03cd8a2ea2f4\".to_string()"
    )]
    pub web_password: String,
    #[source(
        clap(long, help="Backend web cookie key", help_heading = "Web-application config"),
        env = "COOKIE_KEY",
        config = "web.cookie_key",
        default = "\"024e7bbdd3dcf8fcb6e82f7b3edae798661704d8b2e167e1ca680cb9fd4b1ac7\".to_string()"
    )]
    pub cookie_key: String,

    /// Database config
    #[source(
        clap(long, short='H', help="Database domain", help_heading = "Database config"),
        env = "DB_HOST",
        config = "database.host",
        default = "\"db\".to_string()")
    ]
    pub db_host: String,
    #[source(
        clap(long, short='P', help="Database port", help_heading = "Database config"),
        env = "DB_PORT",
        config = "database.port",
        default = 5432)
    ]
    pub db_port: u16,
    #[source(
        clap(long, short='u', help="Database username", help_heading = "Database config"),
        env = "DB_USERNAME",
        config = "database.username",
        default = "\"user\".to_string()")
    ]
    pub db_username: String,
    #[source(
        clap(long, short='p', help="Database password", help_heading = "Database config"),
        env = "DB_PASSWORD",
        config = "database.password",
        default = "\"user\".to_string()")
    ]
    pub db_password: String,
    #[source(
        clap(long, short='s', help="Database schema", help_heading = "Database config"),
        env = "DB_SCHEMA",
        config = "database.schema",
        default = "\"red-eye\".to_string()")
    ]
    pub db_schema: String,

    /// Kafka config
    #[source(
        clap(long, short='b', help="Kafka broker host", help_heading = "Kafka config"),
        env = "KAFKA_HOST",
        config = "kafka.host",
        default = "\"kafka\".to_string()")
    ]
    pub kafka_broker: String,
    #[source(
        clap(long, short='t', help="Kafka broker topic", help_heading = "Kafka config"),
        env = "KAFKA_TOPIC",
        config = "kafka.topic",
        default = "\"raw_packets\".to_string()")
    ]
    pub kafka_topic: String,

    /// Logging configuration
    #[source(
        clap(short, long, help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging config"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "\"trace\".to_string()"
    )]
    pub logging_level: String,
}