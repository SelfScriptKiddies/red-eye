use clap::Parser;

/// `red-eye-agent --version` prints this: crate version + building commit
/// (e.g. "0.1.0+df78fe2"). Same string rides in the heartbeat — see
/// version.rs.
const FULL_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("RED_EYE_GIT_SHA"));

#[derive(Parser, Debug, Clone)]
#[command(name = "red-eye-agent", version = FULL_VERSION, about = "Red Eye traffic capture agent")]
pub struct AgentConfig {
    /// Agent identifier (defaults to hostname)
    #[arg(long, env = "AGENT_ID")]
    pub agent_id: Option<String>,

    /// Network interface to capture on
    #[arg(long, short = 'i', env = "INTERFACE", default_value = "eth0")]
    pub interface: String,

    /// Machine IP address
    #[arg(long, short = 'm', env = "MACHINE_IP", default_value = "0.0.0.0")]
    pub machine_ip: String,

    /// Ports to sniff (comma-separated)
    #[arg(long, short = 'p', env = "PORTS", value_delimiter = ',')]
    pub ports: Vec<u16>,

    /// NATS broker host
    #[arg(long, env = "NATS_HOST", default_value = "nats:4222")]
    pub nats_host: String,

    /// NATS login
    #[arg(long, env = "NATS_LOGIN", default_value = "username")]
    pub nats_login: String,

    /// NATS password
    #[arg(long, env = "NATS_PASSWORD", default_value = "password")]
    pub nats_password: String,

    /// NATS raw packets topic
    #[arg(long, env = "NATS_RAW_PACKETS_TOPIC", default_value = "raw_packets")]
    pub nats_raw_packets_topic: String,

    /// Packets per NATS message (batching; 1 disables batching)
    #[arg(long, env = "NATS_BATCH_SIZE", default_value_t = 32)]
    pub nats_batch_size: usize,

    /// Max ms before a partial packet batch is published
    #[arg(long, env = "NATS_BATCH_INTERVAL_MS", default_value_t = 10)]
    pub nats_batch_interval_ms: u64,

    /// NATS updates topic
    #[arg(long, env = "NATS_UPDATES_TOPIC", default_value = "updates_v3")]
    pub nats_updates_topic: String,

    /// Heartbeat interval in seconds
    #[arg(long, env = "HEARTBEAT_INTERVAL_SECS", default_value_t = 5)]
    pub heartbeat_interval_secs: u64,

    /// Data directory for CA certs and persistent state
    #[arg(long, env = "DATA_DIR", default_value = "/var/lib/red-eye-agent")]
    pub data_dir: String,

    /// Logging level (trace|debug|info|warn|error)
    #[arg(long, short = 'l', env = "LOGGING_LEVEL", default_value = "info")]
    pub logging_level: String,
}

fn sanitize_nats_token(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '.' | '*' | '>' | ' ' | '\t' | '\n' | '\r' => '-',
            c if c.is_ascii_control() => '-',
            _ => c,
        })
        .collect()
}

impl AgentConfig {
    pub fn resolved_agent_id(&self) -> String {
        if let Some(id) = &self.agent_id {
            return sanitize_nats_token(id);
        }

        let ip = &self.machine_ip;
        let raw = match hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
        {
            Some(host) => format!("{}-{}", host, ip),
            None => {
                if ip != "0.0.0.0" {
                    ip.clone()
                } else {
                    uuid::Uuid::new_v4().to_string()
                }
            }
        };
        sanitize_nats_token(&raw)
    }
}
