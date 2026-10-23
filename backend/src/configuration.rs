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
        default = "0.0.0.0".to_string()
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
        default = "".to_string()
    )]
    pub web_password: String,
    #[source(
        clap(long="web-cookie-key", help="Backend web cookie key (min 64 bytes, e.g. `openssl rand -base64 64`)", help_heading = "Web-application config"),
        env = "COOKIE_KEY",
        config = "web.cookie_key",
        default = "".to_string()
    )]
    pub cookie_key: String,

    /// Database config
    #[source(
        clap(long="db-host", short='H', help="Database domain", help_heading = "Database config"),
        env = "DB_HOST",
        config = "database.host",
        default = "db".to_string())
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
        default = "updates_v3".to_string())
    ]
    pub nats_settings_topic: String,

    /// Logging configuration
    #[source(
        clap(short, long="logging-level", help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging config"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "info".to_string()
    )]
    pub logging_level: String,

    /// Cleanup old streams configuration
    #[source(
        clap(long="cleanup-stream-age-minutes", help="Delete non-favorite streams older than N minutes (by last packet time)", help_heading = "Cleanup"),
        env = "CLEANUP_STREAM_AGE_MINUTES",
        config = "cleanup.stream_age_minutes",
        default = 15
    )]
    pub cleanup_stream_age_minutes: i64,
    #[source(
        clap(long="cleanup-interval-minutes", help="Run cleanup every N minutes", help_heading = "Cleanup"),
        env = "CLEANUP_INTERVAL_MINUTES",
        config = "cleanup.interval_minutes",
        default = 2
    )]
    pub cleanup_interval_minutes: u64,
    #[source(
        clap(long="cleanup-max-disk-mb", help="Drop oldest partitions if total size exceeds N MB (0 = disabled)", help_heading = "Cleanup"),
        env = "CLEANUP_MAX_DISK_MB",
        config = "cleanup.max_disk_mb",
        default = 40000
    )]
    pub cleanup_max_disk_mb: i64,

    /// Health monitoring
    #[source(
        clap(long="health-query-warn-ms", help="P95 above this triggers degraded mode", help_heading = "Health"),
        env = "HEALTH_QUERY_WARN_MS",
        config = "health.query_warn_ms",
        default = 3000
    )]
    pub health_query_warn_ms: u64,
    #[source(
        clap(long="health-query-critical-ms", help="P95 above this triggers critical mode", help_heading = "Health"),
        env = "HEALTH_QUERY_CRITICAL_MS",
        config = "health.query_critical_ms",
        default = 8000
    )]
    pub health_query_critical_ms: u64,
    #[source(
        clap(long="health-kill-query-after-ms", help="Kill individual queries running longer than this", help_heading = "Health"),
        env = "HEALTH_KILL_QUERY_AFTER_MS",
        config = "health.kill_query_after_ms",
        default = 30000
    )]
    pub health_kill_query_after_ms: u64,
    /// Frontend settings
    #[source(
        clap(long="max-selected-streams", help="Max streams selectable in multi-select mode", help_heading = "Frontend"),
        env = "MAX_SELECTED_STREAMS",
        config = "frontend.max_selected_streams",
        default = 10
    )]
    pub max_selected_streams: i32,

    /// Version service (/api/version drift checks)
    #[source(
        clap(long="frontend-url", help="Base URL of the frontend (nginx) serving version.json", help_heading = "Version"),
        env = "FRONTEND_URL",
        config = "version.frontend_url",
        default = "http://frontend:80".to_string())
    ]
    pub frontend_url: String,
    #[source(
        clap(long="nats-analyzer-info-subject", help="NATS subject the analyzer answers version requests on", help_heading = "Version"),
        env = "NATS_ANALYZER_INFO_SUBJECT",
        config = "version.analyzer_info_subject",
        default = "service_info.analyzer".to_string())
    ]
    pub nats_analyzer_info_subject: String,

    /// Agent distribution (/api/agent/download)
    #[source(
        clap(long="agent-binary-path", help="Path to the red-eye-agent binary served for download", help_heading = "Agent distribution"),
        env = "AGENT_BINARY_PATH",
        config = "agent.binary_path",
        default = "/usr/local/share/red-eye/red-eye-agent".to_string())
    ]
    pub agent_binary_path: String,
}