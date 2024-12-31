use std::num::NonZero;
use std::string::ToString;
use std::thread;
use clap::ArgMatches;
use dotenv::dotenv;
use log::warn;
use crate::argparse::initialize_cli_args;

const DEFAULT_DB_HOST: &str = "10.10.10.10";
const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_SCHEMA: &str = "red-eye";
const DEFAULT_DB_USERNAME: &str = "user";
const DEFAULT_DB_PASSWORD: &str = "user";

const DEFAULT_INTERFACE: &str = "eth0";
const DEFAULT_MACHINE_IP: &str = "10.10.10.10";

const DEFAULT_THREADS: usize = 4;


pub struct Configuration {
    /// Database parameters
    pub db_host: String,
    pub db_port: u16,
    pub db_schema: String,
    pub db_username: String,
    pub db_password: String,

    /// Client parameters
    pub interface: String,
    pub machine_ip: String,
    pub ports: Vec<u16>,

    /// Workers parameters
    pub available_threads: NonZero<u32>
}

impl std::fmt::Display for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{\"db_host\": \"{}\", \"db_port\": \"{}\", \"db_schema\": \"{}\", \"db_username\": \"{}\", \"db_password\": \"{}\", \"interface\": \"{}\", \"machine_ip\": \"{}\", \"ports\": \"{:?}\", \"available_threads\": {}}}", self.db_host, self.db_port, self.db_schema, self.db_username, self.db_password, self.interface, self.machine_ip, self.ports, self.available_threads)
    }
}

impl std::fmt::Debug for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Configuration(db_host={}, db_port={}, db_schema={}, db_username={}, db_password={}, interface={}, machine_ip={}, ports={:?}, available_threads={})", self.db_host, self.db_port, self.db_schema, self.db_username, self.db_password, self.interface, self.machine_ip, self.ports, self.available_threads)
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
    let allowed_workers = thread::available_parallelism()
        .unwrap_or_else(|e| {
            warn!("Can't get amount of available threads: {e}, defaulting to {DEFAULT_THREADS}");
            NonZero::try_from(DEFAULT_THREADS).unwrap()
        });

    Configuration {
        db_host: initialize_value(&matches, "db_host", "DB_HOST", String::from(DEFAULT_DB_HOST)),
        db_port: initialize_value(&matches, "db_port", "DB_PORT", DEFAULT_DB_PORT),
        db_schema: initialize_value(&matches, "db_schema", "DB_SCHEMA", String::from(DEFAULT_DB_SCHEMA)),
        db_username: initialize_value(&matches, "db_username", "DB_USERNAME", String::from(DEFAULT_DB_USERNAME)),
        db_password: initialize_value(&matches, "db_password", "DB_PASSWORD", String::from(DEFAULT_DB_PASSWORD)),

        interface: initialize_value(&matches, "interface", "INTERFACE", String::from(DEFAULT_INTERFACE)),
        machine_ip: initialize_value(&matches, "machine_ip", "MACHINE_IP", String::from(DEFAULT_MACHINE_IP)),
        ports: vec![],

        available_threads: initialize_value(&matches, "allowed_workers", "ALLOWED_WORKERS", NonZero::try_from(allowed_workers).unwrap()),
    }
}