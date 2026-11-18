// Worker configuration from the environment. The worker holds NO
// database or NATS credentials: it talks only to the backend job API and
// the read-only MCP service.

pub const DEFAULT_BACKEND_URL: &str = "http://backend:8000";
pub const DEFAULT_MCP_URL: &str = "http://red-eye-mcp:8081/mcp";
pub const DEFAULT_WORKER_ID: &str = "soc-worker-1";

#[derive(Debug, Clone)]
pub struct Config {
    pub backend_url: String,
    pub worker_token: String,
    pub mcp_url: String,
    pub mcp_token: String,
    pub worker_id: String,
}

impl Config {
    pub fn from_env() -> Result<Config, String> {
        let backend_url = std::env::var("RED_EYE_BACKEND_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_BACKEND_URL.to_string());
        let worker_token = std::env::var("SOC_WORKER_TOKEN")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or("SOC_WORKER_TOKEN is required (generate in Red Eye Settings)")?;
        let mcp_url = std::env::var("RED_EYE_MCP_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MCP_URL.to_string());
        let mcp_token = std::env::var("MCP_TOKEN")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or("MCP_TOKEN is required (create a read-only MCP token in Red Eye Settings)")?;
        let worker_id = std::env::var("WORKER_ID")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_WORKER_ID.to_string());
        Ok(Config {
            backend_url: backend_url.trim_end_matches('/').to_string(),
            worker_token,
            mcp_url,
            mcp_token,
            worker_id,
        })
    }
}
