use clap::ArgMatches;
use dotenv::dotenv;
use log::warn;
use argparse::initialize_cli_args;

mod argparse;

const DEFAULT_DB_HOST: &str = "db";
const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_USERNAME: &str = "user";
const DEFAULT_DB_PASSWORD: &str = "user";
const DEFAULT_DB_SCHEMA: &str = "red_eye";

const DEFAULT_KAFKA_BROKER_HOST: &str = "kafka:29092";
const DEFAULT_KAFKA_TOPIC: &str = "raw_packets";

#[derive(Debug)]
pub struct Configuration {
    /// Client parameters
    pub db_host: String,
    pub db_port: u16,
    pub db_username: String,
    pub db_password: String,
    pub db_schema: String,

    // /// Filters
    // pub filters: Arc<Mutex<Vec<dyn Fn(&[u8]) -> Option<String>>>>,

    /// Kafka parameters
    pub kafka_broker_host: String,
    pub kafka_topic: String
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
        db_host: initialize_value(&matches, "db_host", "DB_HOST", DEFAULT_DB_HOST.to_string()),
        db_port: initialize_value(&matches, "db_port", "DB_PORT", DEFAULT_DB_PORT),
        db_username: initialize_value(&matches, "db_username", "DB_USERNAME", DEFAULT_DB_USERNAME.to_string()),
        db_password: initialize_value(&matches, "db_password", "DB_PASSWORD", DEFAULT_DB_PASSWORD.to_string()),
        db_schema: initialize_value(&matches, "db_schema", "DB_SCHEMA", DEFAULT_DB_SCHEMA.to_string()),

        // filters: Arc::new(Mutex::new(vec![])),

        kafka_broker_host: initialize_value(&matches, "kafka_host", "BROKER_HOST", DEFAULT_KAFKA_BROKER_HOST.to_string()),
        kafka_topic: initialize_value(&matches, "kafka_topic", "KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC.to_string())

    }
}