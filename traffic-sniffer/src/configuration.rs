use clap::ArgMatches;
use dotenv::dotenv;
use log::warn;
use argparse::initialize_cli_args;

pub mod argparse;

const DEFAULT_INTERFACE: &str = "eth0";
const DEFAULT_MACHINE_IP: &str = "10.10.10.10";

const DEFAULT_KAFKA_BROKER_HOST: &str = "kafka:29092";
const DEFAULT_KAFKA_TOPIC: &str = "raw_packets";

pub struct Configuration {
    /// Client parameters
    pub interface: String,
    pub machine_ip: String,
    pub ports: Vec<u16>,

    /// Kafka parameters
    pub kafka_broker_host: String,
    pub kafka_topic: String
}

impl std::fmt::Display for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{\"interface\": \"{}\", \"machine_ip\": \"{}\", \"ports\": \"{:?}\", \"kafka_broker_host\": \"{}\", \"kafka_topic\": \"{}\"}}", self.interface, self.machine_ip, self.ports, self.kafka_broker_host, self.kafka_topic)
    }
}

impl std::fmt::Debug for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Configuration(interface: {}, machine_ip: {}, ports: {:?}, kafka_broker_host: {}, kafka_topic: {})", self.interface, self.machine_ip, self.ports, self.kafka_broker_host, self.kafka_topic)
    }
}

fn initialize_value<T>(cli_matches: &ArgMatches, cli_name: &str, env_name: &str, default: T) -> T
where
    T: std::str::FromStr + std::fmt::Display + Clone
{
    cli_matches
        .get_one(cli_name).cloned() // CLI Arg check
        .or_else(|| std::env::var(env_name).ok())
        .and_then(|val| val.parse().ok())
        .unwrap_or_else(
            || {
                warn!("Parameter {cli_name} was not set in config, using default value: {default}");
                default
            }
        )
}

/// Current hierarchy of arguments -> CLI-Args > .env > config.toml > default
pub fn init_config() -> Configuration {
    dotenv().ok();
    let matches: ArgMatches = initialize_cli_args().get_matches();

    Configuration {
        interface: initialize_value(&matches, "interface", "INTERFACE", String::from(DEFAULT_INTERFACE)),
        machine_ip: initialize_value(&matches, "machine_ip", "MACHINE_IP", String::from(DEFAULT_MACHINE_IP)),
        ports: vec![],

        kafka_broker_host: initialize_value(&matches, "kafka_broker_host", "BROKER_HOST", String::from(DEFAULT_KAFKA_BROKER_HOST)),
        kafka_topic: initialize_value(&matches, "kafka_topic", "KAFKA_TOPIC", String::from(DEFAULT_KAFKA_TOPIC)),
    }
}