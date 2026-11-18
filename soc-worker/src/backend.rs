use serde_json::{Value, json};
use std::time::Duration;

// Backend job API client (Bearer soc_worker_token). Every call has a
// timeout; failures return typed outcomes the loop can classify instead
// of error-looping.

#[derive(Debug, Clone)]
pub enum JobError {
    /// Provider 4xx schema/authentication: do not retry the identical
    /// request until settings change.
    Permanent(String),
    /// Network, timeout, 429, 5xx: bounded retry with backoff.
    Transient(String),
}

/// Fields the playbook records in logs and submits; some stay unused but
/// mirror the claim contract for future policy work.
#[allow(dead_code)]
pub struct ClaimedJob {
    pub id: i64,
    pub stream_id: i64,
    pub stream_revision: i64,
    pub jev_verdict_id: Option<i64>,
    pub trigger: String,
    pub model: Option<String>,
    pub prompt_version: i32,
    pub attempts: i32,
    pub lease_secs: i64,
    pub max_attempts: i64,
}

#[allow(dead_code)]
pub struct WorkerConfig {
    pub enabled: bool,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub prompt_version: i32,
    pub settings_generation: i64,
    pub max_context_tokens: i64,
    pub max_output_tokens: i64,
    pub max_tool_calls: i64,
    pub max_tool_result_bytes: i64,
    pub job_timeout_secs: i64,
    pub max_attempts: i64,
    pub concurrency: i64,
}

pub struct Backend {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

pub enum ClaimOutcome {
    Job(ClaimedJob),
    Empty,
    /// Claims paused (SOC disabled or daily limit reached): explicit
    /// pause state, never an error loop.
    Paused(String),
    Failed(JobError),
}

impl Backend {
    pub fn new(base_url: &str, token: &str) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Backend {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
        })
    }

    fn auth(&self) -> reqwest::header::HeaderValue {
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", self.token))
            .unwrap_or(reqwest::header::HeaderValue::from_static("Bearer invalid"))
    }

    async fn post(&self, path: &str, body: Value) -> Result<(u16, Value), JobError> {
        let resp = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .header("Authorization", self.auth())
            .json(&body)
            .send()
            .await
            .map_err(|e| JobError::Transient(short(&e.to_string())))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        let v = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
        Ok((status, v))
    }

    async fn get(&self, path: &str) -> Result<(u16, Value), JobError> {
        let resp = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header("Authorization", self.auth())
            .send()
            .await
            .map_err(|e| JobError::Transient(short(&e.to_string())))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        let v = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
        Ok((status, v))
    }

    pub async fn worker_config(&self) -> Result<WorkerConfig, JobError> {
        let (status, v) = self.get("/api/soc/worker-config").await?;
        if status != 200 {
            return Err(JobError::Transient(format!(
                "worker-config status {}",
                status
            )));
        }
        let limits = &v["limits"];
        Ok(WorkerConfig {
            enabled: v["enabled"].as_bool().unwrap_or(false),
            base_url: v["base_url"].as_str().unwrap_or("").to_string(),
            api_key: v["api_key"].as_str().unwrap_or("").to_string(),
            model: v["model"].as_str().unwrap_or("").to_string(),
            prompt_version: v["prompt_version"].as_i64().unwrap_or(0) as i32,
            settings_generation: v["settings_generation"].as_i64().unwrap_or(0),
            max_context_tokens: limits["max_context_tokens"].as_i64().unwrap_or(16384),
            max_output_tokens: limits["max_output_tokens"].as_i64().unwrap_or(1024),
            max_tool_calls: limits["max_tool_calls"].as_i64().unwrap_or(8),
            max_tool_result_bytes: limits["max_tool_result_bytes"].as_i64().unwrap_or(16384),
            job_timeout_secs: limits["job_timeout_secs"].as_i64().unwrap_or(120),
            max_attempts: limits["max_attempts"].as_i64().unwrap_or(3),
            concurrency: limits["concurrency"].as_i64().unwrap_or(1),
        })
    }

    pub async fn claim(&self, worker_id: &str, job_timeout_secs: i64) -> ClaimOutcome {
        match self
            .post(
                "/api/soc/jobs/claim",
                json!({"worker_id": worker_id, "lease_secs": job_timeout_secs}),
            )
            .await
        {
            Ok((200, v)) => {
                let job = &v["job"];
                ClaimOutcome::Job(ClaimedJob {
                    id: job["id"].as_i64().unwrap_or(0),
                    stream_id: job["stream_id"].as_i64().unwrap_or(0),
                    stream_revision: job["stream_revision"].as_i64().unwrap_or(0),
                    jev_verdict_id: job["jev_verdict_id"].as_i64(),
                    trigger: job["trigger"].as_str().unwrap_or("manual").to_string(),
                    model: job["model"].as_str().map(|s| s.to_string()),
                    prompt_version: job["prompt_version"].as_i64().unwrap_or(0) as i32,
                    attempts: job["attempts"].as_i64().unwrap_or(0) as i32,
                    lease_secs: v["lease_secs"].as_i64().unwrap_or(120),
                    max_attempts: v["max_attempts"].as_i64().unwrap_or(3),
                })
            }
            Ok((204, _)) => ClaimOutcome::Empty,
            Ok((409, v)) => {
                ClaimOutcome::Paused(v["reason"].as_str().unwrap_or("claims paused").to_string())
            }
            Ok((status, _)) => {
                ClaimOutcome::Failed(JobError::Transient(format!("claim status {}", status)))
            }
            Err(e) => ClaimOutcome::Failed(e),
        }
    }

    /// Record the ids this job actually fetched through MCP. Result
    /// evidence must reference only these.
    pub async fn record_fetched(&self, job_id: i64, entries: &[String]) -> Result<(), JobError> {
        let (status, _) = self
            .post(
                &format!("/api/soc/jobs/{}/fetched", job_id),
                json!({"entries": entries}),
            )
            .await?;
        if status == 200 {
            Ok(())
        } else {
            Err(JobError::Transient(format!("fetched status {}", status)))
        }
    }

    pub async fn submit_result(&self, job_id: i64, result: &Value) -> Result<(), JobError> {
        let (status, v) = self
            .post(&format!("/api/soc/jobs/{}/result", job_id), result.clone())
            .await?;
        match status {
            200 => Ok(()),
            422 => {
                let code = v["error"]["code"].as_str().unwrap_or("");
                let message = v["error"]["message"].as_str().unwrap_or("rejected");
                if code == "stale_generation" || code == "job_not_active" {
                    // Terminal: retrying cannot help.
                    Err(JobError::Permanent(format!("{}: {}", code, message)))
                } else {
                    Err(JobError::Permanent(format!("rejected: {}", message)))
                }
            }
            401 | 403 => Err(JobError::Permanent("worker credential rejected".into())),
            status => Err(JobError::Transient(format!("result status {}", status))),
        }
    }

    pub async fn report_failure(&self, job_id: i64, error: &JobError) -> Result<(), JobError> {
        let (permanent, text) = match error {
            JobError::Permanent(t) => (true, t.clone()),
            JobError::Transient(t) => (false, t.clone()),
        };
        let (status, _) = self
            .post(
                &format!("/api/soc/jobs/{}/failure", job_id),
                json!({"error": text, "permanent": permanent}),
            )
            .await?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(JobError::Transient(format!(
                "failure report status {}",
                status
            )))
        }
    }
}

pub fn short(s: &str) -> String {
    s.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claimed_job_parses_from_claim_response_shape() {
        let raw = json!({
            "job": {
                "id": 7, "stream_id": 3, "stream_revision": 900,
                "jev_verdict_id": null, "trigger": "manual",
                "model": "m", "prompt_version": 1, "attempts": 2,
            },
            "lease_secs": 90, "max_attempts": 3,
        });
        // Mirror of Backend::claim's field extraction.
        let job = &raw["job"];
        assert_eq!(job["id"].as_i64(), Some(7));
        assert_eq!(job["jev_verdict_id"].as_i64(), None);
        assert_eq!(job["attempts"].as_i64(), Some(2));
        assert_eq!(raw["lease_secs"].as_i64(), Some(90));
    }
}
