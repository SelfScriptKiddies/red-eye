use clap::ArgMatches;
use dotenv::dotenv;
use log::warn;
use crate::argparse::initialize_cli_args;

pub struct Configuration {
    /// Database parameters
    pub db_host: String,
    pub db_port: u16,
    pub db_username: String,
    pub db_password: String,

    /// Client parameters
    pub interface: String,
    pub machine_ip: String,
    pub ports: Vec<u16>
}

impl std::fmt::Display for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{\"db_host\": \"{}\", \"db_port\": \"{}\", \"db_username\": \"{}\", \"db_password\": \"{}\", \"interface\": \"{}\", \"machine_ip\": \"{}\", \"ports\": \"{:?}\"}}", self.db_host, self.db_port, self.db_username, self.db_password, self.interface, self.machine_ip, self.ports)
    }
}

impl std::fmt::Debug for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Configuration(db_host={}, db_port={}, db_username={}, db_password={}, interface={}, machine_ip={}, ports: {:?})", self.db_host, self.db_port, self.db_username, self.db_password, self.interface, self.machine_ip, self.ports)
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
        db_host: initialize_value(&matches, "db_host", "DB_HOST", "db".to_string()),
        db_port: initialize_value(&matches, "db_port", "DB_PORT", 5432),
        db_username: initialize_value(&matches, "db_username", "DB_USERNAME", "user".to_string()),
        db_password: initialize_value(&matches, "db_password", "DB_PASSWORD", "user".to_string()),

        interface: initialize_value(&matches, "interface", "INTERFACE", "eth0".to_string()),
        machine_ip: initialize_value(&matches, "machine_ip", "MACHINE_IP", "10.10.10.10".to_string()),
        ports: vec![]
    }
}