use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "red-eye-agent", version, about = "Red Eye traffic capture agent")]
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

    /// NATS updates topic
    #[arg(long, env = "NATS_UPDATES_TOPIC", default_value = "updates")]
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

impl AgentConfig {
    pub fn resolved_agent_id(&self) -> String {
        if let Some(id) = &self.agent_id {
            return id.clone();
        }

        // Build "hostname-ip" to avoid collisions across machines with the same hostname
        // (e.g. 3 VMs all named "debian").  Fall back to just the IP if hostname is
        // unavailable, and finally to a random UUID if even the IP is unset.
        let ip = &self.machine_ip;
        match hostname::get()
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
        }
    }
}
