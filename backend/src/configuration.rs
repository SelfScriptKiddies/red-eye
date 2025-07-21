use config_manager::config;

#[derive(Debug, Clone)]
#[config(
    clap(version, author),
    file(
        format = "toml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "demo_config",
        optional
    )
)]
pub struct ApplicationConfig {
    /// Web-application config
    #[source(
        clap(long="web-host", help="Backend web ip", help_heading = "Web-application config"),
        env = "WEB_IP",
        config = "web.ip",
        default = "\"0.0.0.0\".to_string()"
    )]
    pub web_host: String,
    #[source(
        clap(long="web-port", help="Backend web port", help_heading = "Web-application config"),
        env = "WEB_PORT",
        config = "web.port",
        default = 8080
    )]
    pub web_port: u16,
    #[source(
        clap(long="web-password", help="Backend web password for login", help_heading = "Web-application config"),
        env = "WEB_PASSWORD",
        config = "web.password",
        default = "\"fbfd83ebcabfd05ace9c03cd8a2ea2f4\".to_string()"
    )]
    pub web_password: String,
    #[source(
        clap(long="web-cookie-key", help="Backend web cookie key", help_heading = "Web-application config"),
        env = "COOKIE_KEY",
        config = "web.cookie_key",
        default = "\"024e7bbdd3dcf8fcb6e82f7b3edae798661704d8b2e167e1ca680cb9fd4b1ac7\".to_string()"
    )]
    pub cookie_key: String,

    /// Database config
    #[source(
        clap(long="db-host", short='H', help="Database domain", help_heading = "Database config"),
        env = "DB_HOST",
        config = "database.host",
        default = "\"db\".to_string()")
    ]
    pub db_host: String,
    #[source(
        clap(long="db-port", short='P', help="Database port", help_heading = "Database config"),
        env = "DB_PORT",
        config = "database.port",
        default = 5432)
    ]
    pub db_port: u16,
    #[source(
        clap(long="db-username", short='u', help="Database username", help_heading = "Database config"),
        env = "DB_USERNAME",
        config = "database.username",
        default = "username".to_string())
    ]
    pub db_username: String,
    #[source(
        clap(long="db-password", short='p', help="Database password", help_heading = "Database config"),
        env = "DB_PASSWORD",
        config = "database.password",
        default = "password".to_string())
    ]
    pub db_password: String,
    #[source(
        clap(long="db-schema", short='s', help="Database schema", help_heading = "Database config"),
        env = "DB_SCHEMA",
        config = "database.schema",
        default = "red-eye".to_string())
    ]
    pub db_schema: String,

    /// NATS config
    #[source(
        clap(long="nats-host", short='b', help="NATS broker host", help_heading = "NATS config"),
        env = "NATS_HOST",
        config = "nats.host",
        default = "nats:4222".to_string())
    ]
    pub nats_broker: String,
    #[source(
        clap(long="nats-login", help="NATS login", help_heading = "NATS config"),
        env = "NATS_LOGIN",
        config = "nats.login",
        default = "username".to_string())
    ]
    pub nats_login: String,
    #[source(
        clap(long="nats-password", help="NATS password", help_heading = "NATS config"),
        env = "NATS_PASSWORD",
        config = "nats.password",
        default = "password".to_string())
    ]
    pub nats_password: String,
    #[source(
        clap(long="nats-websocket-topic", short='R', help="NATS websocket topic", help_heading = "NATS config"),
        env = "NATS_WEBSOCKET_TOPIC",
        config = "nats.websocket_topic",
        default = "websocket_packets".to_string())
    ]
    pub nats_websocket_topic: String,
    #[source(
        clap(long="nats-settings-topic", short='S', help="NATS settings topic", help_heading = "NATS config"),
        env = "NATS_SETTINGS_TOPIC",
        config = "nats.settings_topic",
        default = "updates".to_string())
    ]
    pub nats_settings_topic: String,

    /// Logging configuration
    #[source(
        clap(short, long="logging-level", help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging config"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "trace".to_string()
    )]
    pub logging_level: String,
}