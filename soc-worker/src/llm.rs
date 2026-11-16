use serde_json::{Value, json};
use std::time::Duration;

use crate::backend::JobError;

// OpenAI-compatible /chat/completions client for a self-hosted endpoint.
// temperature 0 + fixed seed when accepted; structured output via
// response_format (json_schema preferred, json_object fallback, bare
// parsing as the last resort). Every response is validated by the caller;
// a malformed document is never interpreted as benign.

pub struct LlmClient {
    http: reqwest::Client,
}

pub struct ChatResult {
    pub content: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

impl LlmClient {
    pub fn new() -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(LlmClient { http })
    }

    /// One completion. `structured` drives response_format; a provider
    /// that rejects the field is retried once without it (grammar
    /// fallback), and the caller still validates the output.
    #[allow(clippy::too_many_arguments)]
    pub async fn chat(
        &self,
        base_url: &str,
        api_key: &str,
        model: &str,
        system: &str,
        user: &str,
        max_output_tokens: i64,
        structured: Option<(&'static str, Value)>,
    ) -> Result<ChatResult, JobError> {
        let mut body = json!({
            "model": model,
            "temperature": 0,
            "seed": 42,
            "max_tokens": max_output_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        });
        if let Some((kind, schema)) = &structured {
            body["response_format"] = if *kind == crate::prompts::SCHEMA_MODE {
                json!({"type": "json_schema", "json_schema": schema})
            } else {
                json!({"type": "json_object"})
            };
        }

        let (status, text) = self.send(base_url, api_key, &body).await?;
        // 400 with response_format present -> one retry without it.
        if status == 400 && structured.is_some() {
            let mut retry = body.clone();
            retry.as_object_mut().unwrap().remove("response_format");
            let (status2, text2) = self.send(base_url, api_key, &retry).await?;
            return finish(status2, text2);
        }
        finish(status, text)
    }

    async fn send(
        &self,
        base_url: &str,
        api_key: &str,
        body: &Value,
    ) -> Result<(u16, String), JobError> {
        let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
        let mut req = self.http.post(&url).json(body);
        if !api_key.trim().is_empty() {
            req = req.header("Authorization", format!("Bearer {}", api_key.trim()));
        }
        let resp = req.send().await.map_err(|e| {
            JobError::Transient(format!(
                "llm network: {}",
                crate::backend::short(&e.to_string())
            ))
        })?;
        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| JobError::Transient(format!("llm read: {}", e)))?;
        Ok((status, text))
    }
}

fn finish(status: u16, text: String) -> Result<ChatResult, JobError> {
    match status {
        200 => {}
        // Provider-side rejection of our request shape (bad model name,
        // context overflow, auth): retrying the identical request cannot
        // succeed.
        400 | 401 | 403 | 404 | 422 => {
            return Err(JobError::Permanent(format!(
                "llm rejected ({}): {}",
                status,
                crate::backend::short(&text)
            )));
        }
        429 | 500..=599 => {
            return Err(JobError::Transient(format!("llm status {}", status)));
        }
        other => return Err(JobError::Transient(format!("llm status {}", other))),
    }
    let v: Value = serde_json::from_str(&text)
        .map_err(|_| JobError::Permanent("llm returned non-JSON body".into()))?;
    let content = v["choices"][0]["message"]["content"]
        .as_str()
        .ok_or(JobError::Permanent(
            "llm response missing choices[0].message.content".into(),
        ))?
        .to_string();
    Ok(ChatResult {
        content,
        input_tokens: v["usage"]["prompt_tokens"].as_i64(),
        output_tokens: v["usage"]["completion_tokens"].as_i64(),
    })
}

/// Extract a JSON document from model output: raw JSON, or markdown-fenced
/// JSON. Anything else is malformed (never defaulted to benign).
pub fn extract_json(content: &str) -> Result<Value, JobError> {
    let trimmed = content.trim();
    let candidate = if trimmed.starts_with('{') {
        trimmed.to_string()
    } else if let Some(start) = trimmed.find('{') {
        let end = trimmed.rfind('}').unwrap_or(start);
        if end > start {
            trimmed[start..=end].to_string()
        } else {
            return Err(JobError::Permanent(
                "malformed model output: no JSON object".into(),
            ));
        }
    } else {
        return Err(JobError::Permanent(format!(
            "malformed model output: {}",
            crate::backend::short(trimmed)
        )));
    };
    serde_json::from_str(&candidate)
        .map_err(|_| JobError::Permanent("malformed model output: unparsable JSON".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_accepts_raw_and_fenced_but_not_prose() {
        let raw = r#"{"verdict": "unknown"}"#;
        assert_eq!(extract_json(raw).unwrap()["verdict"], "unknown");
        let fenced = "```json\n{\"verdict\": \"benign\"}\n```";
        assert_eq!(extract_json(fenced).unwrap()["verdict"], "benign");
        let prose = "The stream looks suspicious to me, no JSON here.";
        assert!(extract_json(prose).is_err());
        assert!(extract_json("").is_err());
    }
}
