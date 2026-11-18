use serde_json::{Value, json};

use crate::config::{Config, backend_timeout};

// The only way red-eye-mcp talks to Red Eye: bounded backend HTTP calls
// with its service credential. It holds NO database credentials and no
// other backend path exists from here. Backend failures fail closed and
// are reported to the caller without retry loops inside this service.

#[derive(Debug, Clone, serde::Serialize)]
pub struct TokenIdentity {
    pub token_id: i64,
    pub capability: String,
    pub name: String,
}

pub enum BackendError {
    /// 401/403 from the backend (invalid service credential).
    Forbidden,
    /// Token not valid (authz 401).
    TokenRejected,
    /// 404 from a resource endpoint.
    NotFound,
    /// 409/422 typed rejection bodies from control endpoints.
    Rejected { status: u16, message: String },
    /// Timeouts, connection failures, 5xx: fail closed.
    Unavailable(String),
}

pub struct BackendClient {
    http: reqwest::Client,
    base_url: String,
    service_token: String,
}

impl BackendClient {
    pub fn new(config: &Config) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(backend_timeout())
            .build()
            .map_err(|e| format!("backend client build failed: {}", e))?;
        Ok(BackendClient {
            http,
            base_url: config.backend_url.clone(),
            service_token: config.service_token.clone(),
        })
    }

    /// No credential configured: the whole service is disabled. Authz
    /// fails closed for every presented token.
    pub fn is_disabled(&self) -> bool {
        self.service_token.trim().is_empty()
    }

    async fn get(&self, path: &str) -> Result<Value, BackendError> {
        let resp = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("Bearer {}", self.service_token))
            .send()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))?;
        handle_status(resp)
            .await?
            .json::<Value>()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, BackendError> {
        let resp = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("Bearer {}", self.service_token))
            .json(&body)
            .send()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))?;
        handle_status(resp)
            .await?
            .json::<Value>()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))
    }

    async fn put(&self, path: &str, body: Value) -> Result<Value, BackendError> {
        let resp = self
            .http
            .put(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("Bearer {}", self.service_token))
            .json(&body)
            .send()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))?;
        handle_status(resp)
            .await?
            .json::<Value>()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))
    }

    async fn delete(&self, path: &str, body: Value) -> Result<Value, BackendError> {
        let resp = self
            .http
            .delete(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("Bearer {}", self.service_token))
            .json(&body)
            .send()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))?;
        handle_status(resp)
            .await?
            .json::<Value>()
            .await
            .map_err(|e| BackendError::Unavailable(short_err(&e)))
    }

    /// Validate a presented MCP bearer token. Any backend failure fails
    /// closed: no identity, no fallback.
    pub async fn authorize(&self, presented_token: &str) -> Result<TokenIdentity, BackendError> {
        if self.is_disabled() {
            return Err(BackendError::Unavailable(
                "service credential not configured".into(),
            ));
        }
        match self
            .post(
                "/api/mcp/tokens/authz",
                json!({"token": presented_token.trim()}),
            )
            .await
        {
            Ok(v) => Ok(TokenIdentity {
                token_id: v["token_id"].as_i64().unwrap_or(0),
                capability: v["capability"].as_str().unwrap_or("").to_string(),
                name: v["name"].as_str().unwrap_or("").to_string(),
            }),
            Err(BackendError::NotFound) => Err(BackendError::TokenRejected),
            Err(BackendError::Forbidden) => Err(BackendError::TokenRejected),
            Err(e) => Err(e),
        }
    }

    pub async fn list_services(&self) -> Result<Value, BackendError> {
        self.get("/api/mcp/services").await
    }

    pub async fn list_agents(&self) -> Result<Value, BackendError> {
        self.get("/api/mcp/agents").await
    }

    pub async fn search_streams(&self, args: &Value) -> Result<Value, BackendError> {
        let mut q = Vec::new();
        for (arg, key) in [
            ("service_id", "service_id"),
            ("agent_id", "agent_id"),
            ("tag_id", "tag_id"),
            ("ip", "ip"),
            ("protocol", "protocol"),
            ("from_secs", "from_secs"),
            ("to_secs", "to_secs"),
            ("cursor_time", "cursor_time"),
            ("cursor_id", "cursor_id"),
        ] {
            if let Some(v) = args.get(arg).filter(|v| !v.is_null()) {
                q.push(format!("{}={}", key, urlencode(&v.to_string())));
            }
        }
        if args.get("favorite").and_then(|v| v.as_bool()) == Some(true) {
            q.push("favorite=true".to_string());
        }
        let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20);
        q.push(format!("limit={}", limit.clamp(1, 100)));
        self.get(&format!("/api/mcp/streams?{}", q.join("&"))).await
    }

    pub async fn stream_summary(&self, stream_id: i64) -> Result<Value, BackendError> {
        self.get(&format!("/api/mcp/stream/{}/summary", stream_id))
            .await
    }

    pub async fn stream_messages(
        &self,
        stream_id: i64,
        limit: u64,
        max_entry_bytes: u64,
    ) -> Result<Value, BackendError> {
        self.get(&format!(
            "/api/mcp/stream/{}/messages?limit={}&max_entry_bytes={}",
            stream_id,
            limit.clamp(1, 100),
            max_entry_bytes.clamp(128, 16384)
        ))
        .await
    }

    pub async fn get_packet(&self, packet_id: i64, max_bytes: u64) -> Result<Value, BackendError> {
        self.get(&format!(
            "/api/mcp/packet/{}?max_bytes={}",
            packet_id,
            max_bytes.clamp(128, 65536)
        ))
        .await
    }

    pub async fn get_message_headers(
        &self,
        message_id: i64,
        max_bytes: u64,
    ) -> Result<Value, BackendError> {
        self.get(&format!(
            "/api/mcp/message/{}/headers?max_bytes={}",
            message_id,
            max_bytes.clamp(128, 16384)
        ))
        .await
    }

    pub async fn stream_tags(&self, stream_id: i64) -> Result<Value, BackendError> {
        self.get(&format!("/api/mcp/stream/{}/tags", stream_id))
            .await
    }

    pub async fn stream_jev_verdicts(
        &self,
        stream_id: i64,
        limit: u64,
    ) -> Result<Value, BackendError> {
        self.get(&format!(
            "/api/mcp/stream/{}/jev-verdicts?limit={}",
            stream_id,
            limit.clamp(1, 100)
        ))
        .await
    }

    pub async fn related_streams(
        &self,
        stream_id: i64,
        window_secs: u64,
        limit: u64,
    ) -> Result<Value, BackendError> {
        self.get(&format!(
            "/api/mcp/related-streams?stream_id={}&window_secs={}&limit={}",
            stream_id,
            window_secs.clamp(1, 3600),
            limit.clamp(1, 20)
        ))
        .await
    }

    pub async fn get_soc_result(&self, result_id: i64) -> Result<Value, BackendError> {
        self.get(&format!("/api/mcp/soc-result/{}", result_id))
            .await
    }

    pub async fn component_health(&self) -> Result<Value, BackendError> {
        self.get("/api/mcp/health").await
    }

    // ----- Full-control mutations (backend enforces capability, revision,
    // confirm and reason independently of this process). -----

    async fn mutate(&self, path: &str, body: Value) -> Result<Value, BackendError> {
        self.post(path, body).await
    }

    pub async fn create_service(&self, body: Value) -> Result<Value, BackendError> {
        self.mutate("/api/mcp/control/service", body).await
    }

    pub async fn update_service(&self, id: i64, body: Value) -> Result<Value, BackendError> {
        self.put(&format!("/api/mcp/control/service/{}", id), body)
            .await
    }

    pub async fn delete_service(&self, id: i64, base: Value) -> Result<Value, BackendError> {
        self.delete(&format!("/api/mcp/control/service/{}", id), base)
            .await
    }

    pub async fn set_agent_network_settings(
        &self,
        agent_id: &str,
        body: Value,
    ) -> Result<Value, BackendError> {
        self.put(
            &format!(
                "/api/mcp/control/agent/{}/network-settings",
                urlencode(agent_id)
            ),
            body,
        )
        .await
    }

    pub async fn set_proxy_protection(
        &self,
        service_id: i64,
        body: Value,
    ) -> Result<Value, BackendError> {
        self.mutate(
            &format!("/api/mcp/control/service/{}/proxy", service_id),
            body,
        )
        .await
    }

    pub async fn create_filter(&self, body: Value) -> Result<Value, BackendError> {
        self.mutate("/api/mcp/control/filter", body).await
    }

    pub async fn update_filter(&self, id: i64, body: Value) -> Result<Value, BackendError> {
        self.put(&format!("/api/mcp/control/filter/{}", id), body)
            .await
    }

    pub async fn set_filter_enabled(&self, id: i64, body: Value) -> Result<Value, BackendError> {
        self.put(&format!("/api/mcp/control/filter/{}/enabled", id), body)
            .await
    }

    pub async fn delete_filter(&self, id: i64, base: Value) -> Result<Value, BackendError> {
        self.delete(&format!("/api/mcp/control/filter/{}", id), base)
            .await
    }

    pub async fn remove_active_block(&self, body: Value) -> Result<Value, BackendError> {
        self.mutate("/api/mcp/control/blocks/remove", body).await
    }
}

async fn handle_status(resp: reqwest::Response) -> Result<reqwest::Response, BackendError> {
    let status = resp.status().as_u16();
    match status {
        200 | 201 => Ok(resp),
        204 => Ok(resp),
        401 | 403 => Err(BackendError::Forbidden),
        404 => Err(BackendError::NotFound),
        409 | 422 => {
            let body = resp.text().await.unwrap_or_default();
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| body.chars().take(200).collect());
            Err(BackendError::Rejected { status, message })
        }
        other => Err(BackendError::Unavailable(format!(
            "backend status {}",
            other
        ))),
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn short_err(e: &reqwest::Error) -> String {
    e.to_string().chars().take(160).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencode_escapes_specials() {
        assert_eq!(urlencode("10.0.0.1"), "10.0.0.1");
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode("x&y=z"), "x%26y%3Dz");
    }
}
