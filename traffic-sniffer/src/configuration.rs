use config_manager::config;
use serde::Deserialize;

pub mod updater;

#[derive(Debug, Clone)]
#[config(
    clap(version, author),
    file(
        format = "yaml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "demo_config",
        optional
    )
)]
pub struct ApplicationConfig {
    /// Client parameters
    #[source(
        clap(long="interface", short='i', help="Network interface", help_heading = "Client parameters"),
        env = "INTERFACE",
        config = "interface",
        default = "eth0".to_string()
    )]
    pub interface: String,
    #[source(
        clap(long="machine-ip", short='m', help="Machine IP address", help_heading = "Client parameters"),
        env = "MACHINE_IP",
        config = "machine_ip",
        default = "0.0.0.0".to_string()
    )]
    pub machine_ip: String,
    #[source(
        clap(long="ports", short='p', help="Ports to sniff", help_heading = "Client parameters"),
        env = "PORTS",
        config = "ports",
        default = PortList(vec![31337])
    )]
    pub ports: PortList,

    /// NATS parameters
    #[source(
        clap(long="nats-host", short='b', help="NATS broker host", help_heading = "NATS parameters"),
        env = "NATS_HOST",
        config = "nats.host",
        default = "nats:4222".to_string()
    )]
    pub nats_host: String,
    #[source(
        clap(long="nats-login", help="NATS login", help_heading = "NATS parameters"),
        env = "NATS_LOGIN",
        config = "nats.login",
        default = "username".to_string()
    )]
    pub nats_login: String,
    #[source(
        clap(long="nats-password", help="NATS password", help_heading = "NATS parameters"),
        env = "NATS_PASSWORD",
        config = "nats.password",
        default = "password".to_string()
    )]
    pub nats_password: String,
    #[source(
        clap(long="nats-raw-packets-topic", short='R', help="NATS raw packets topic", help_heading = "NATS parameters"),
        env = "NATS_RAW_PACKETS_TOPIC",
        config = "nats.raw_packets_topic",
        default = "raw_packets".to_string()
    )]
    pub nats_raw_packets_topic: String,
    #[source(
        clap(long="nats-updates-topic", short='U', help="NATS updates topic", help_heading = "NATS parameters"),
        env = "NATS_UPDATES_TOPIC",
        config = "nats.updates_topic",
        default = "updates".to_string()
    )]
    pub nats_updates_topic: String,

    /// Service UUID
    #[source(
        clap(long="service-uuid", help="Service UUID", help_heading = "Service options"),
        env = "SERVICE_UUID",
        config = "service.uuid",
        default = uuid::Uuid::new_v4().to_string()
    )]
    pub service_uuid: String,

    /// Logging configuration
    #[source(
        clap(short, long="logging-level", help="Logging level (trace|debug|info|warn|error)", help_heading = "Logging config"),
        env = "LOGGING_LEVEL",
        config = "logging.level",
        default = "trace".to_string()
    )]
    pub logging_level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PortList(pub Vec<u16>);

impl std::str::FromStr for PortList {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ports: Result<Vec<u16>, _> = s
            .split(',')
            .map(|p| p.trim().parse::<u16>())
            .collect();

        match ports {
            Ok(port_list) => Ok(PortList(port_list)),
            Err(e) => Err(format!("Failed to parse port list: {}", e))
        }
    }
}
