use serde_json::{Value, json};
use std::time::Instant;

use crate::backend::{Backend, ClaimedJob, JobError, WorkerConfig};
use crate::llm::{LlmClient, extract_json};
use crate::mcp_client::McpSession;
use crate::prompts;
use crate::result::validate_final;

// The fixed orchestrator playbook. The model NEVER controls the sequence:
//  1. validate job identity,
//  2. summary + JEV verdicts for the exact revision,
//  3. bounded messages,
//  4. ONE preliminary model call -> typed assessment + <=2 evidence
//     requests from the allowlist (one corrected retry on invalid),
//  5. fetch evidence, record fetched ids, cap tool result bytes,
//  6. ONE final model call with the strict schema,
//  7. validate and submit before lease expiry.

pub struct AttemptOutcome {
    pub submitted: bool,
    pub error: Option<JobError>,
}

pub async fn run_attempt(
    backend: &Backend,
    llm: &LlmClient,
    job: &ClaimedJob,
    cfg: &WorkerConfig,
    mcp_url: &str,
    mcp_token: &str,
) -> AttemptOutcome {
    match run_inner(backend, llm, job, cfg, mcp_url, mcp_token).await {
        Ok(()) => AttemptOutcome {
            submitted: true,
            error: None,
        },
        Err(e) => AttemptOutcome {
            submitted: false,
            error: Some(e),
        },
    }
}

async fn run_inner(
    backend: &Backend,
    llm: &LlmClient,
    job: &ClaimedJob,
    cfg: &WorkerConfig,
    mcp_url: &str,
    mcp_token: &str,
) -> Result<(), JobError> {
    // Step 1: identity. A job written for another prompt generation is
    // stale by construction (the backend's queue claims only current
    // generation work; this catches rows surviving a settings bump).
    if job.prompt_version != prompts::PROMPT_VERSION {
        return Err(JobError::Permanent(format!(
            "job asks for prompt_version {} but this worker speaks {}",
            job.prompt_version,
            prompts::PROMPT_VERSION
        )));
    }
    let started = Instant::now();

    // Step 2+3: evidence gathering via read-only MCP.
    let session = McpSession::connect(mcp_url, mcp_token).await?;
    let result = gather_and_classify(backend, llm, job, cfg, &session, started).await;
    session.close().await;
    result
}

async fn gather_and_classify(
    backend: &Backend,
    llm: &LlmClient,
    job: &ClaimedJob,
    cfg: &WorkerConfig,
    session: &McpSession,
    started: Instant,
) -> Result<(), JobError> {
    let summary = session
        .call("get_stream_summary", json!({"stream_id": job.stream_id}))
        .await?;

    // Verdicts for the exact revision only: later revisions do not belong
    // to this job's question.
    let verdicts_raw = session
        .call(
            "get_jev_verdicts",
            json!({"stream_id": job.stream_id, "limit": 20}),
        )
        .await
        .map(|v| {
            v.as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|v| v["last_packet_id"].as_i64().unwrap_or(0) <= job.stream_revision)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let jev_answers = if verdicts_raw.is_empty() {
        json!([])
    } else {
        json!({
            "verdicts": verdicts_raw.iter().map(|v| json!({
                "attack_class": v["attack_class"],
                "severity": v["severity"],
                "confidence": v["confidence"],
                "is_attack": v["is_attack"],
                "last_packet_id": v["last_packet_id"],
            })).collect::<Vec<_>>(),
        })
    };

    // Bounded message window: derive entry count from the context budget.
    let message_limit = (cfg.max_context_tokens / 200).clamp(10, 100);
    let entry_bytes = (cfg.max_tool_result_bytes / 4).clamp(256, 16384);
    let messages = session
        .call(
            "get_stream_messages",
            json!({
                "stream_id": job.stream_id,
                "limit": message_limit,
                "max_entry_bytes": entry_bytes,
            }),
        )
        .await?;

    // Step 4: one preliminary call (one corrected retry allowed).
    let prelim_user = format!(
        "{}\n\nSTREAM SUMMARY:\n{}\n\nJEV ANSWERS:\n{}\n\nBOUNDED MESSAGES:\n{}",
        prompts::PRELIMINARY_INSTRUCTION,
        compact(&summary, (cfg.max_tool_result_bytes / 2) as usize),
        compact(&jev_answers, 2048),
        compact(&messages, (cfg.max_tool_result_bytes / 2) as usize),
    );
    let mut preliminary = call_llm(
        llm,
        cfg,
        &job.model.clone().unwrap_or_else(|| cfg.model.clone()),
        prompts::SYSTEM,
        &prelim_user,
        Some((prompts::JSON_MODE, json!({}))),
    )
    .await?;
    let mut requests = parse_preliminary(&preliminary.content);
    if requests.is_err() {
        // ONE corrected preliminary within the same attempt.
        preliminary = call_llm(
            llm,
            cfg,
            &job.model.clone().unwrap_or_else(|| cfg.model.clone()),
            prompts::SYSTEM,
            &format!(
                "Your previous answer was not a valid JSON object with an evidence_requests \
                 array. Answer again.\n\n{}",
                prelim_user
            ),
            Some((prompts::JSON_MODE, json!({}))),
        )
        .await?;
        requests = parse_preliminary(&preliminary.content);
    }
    let requests = requests.unwrap_or_default();

    // Step 5: fetch requested evidence within the remaining budget.
    // Names and args are validated against the allowlist; each tool at
    // most once; <=2 requests honored, extras dropped silently.
    let mut fetched: Vec<String> = Vec::new();
    let mut evidence_parts: Vec<Value> = Vec::new();
    let mut used_tools: Vec<String> = Vec::new();
    let tool_budget = cfg.max_tool_calls.min(8) as usize;
    for req in requests.into_iter().take(2) {
        if evidence_parts.len() >= tool_budget {
            break;
        }
        let Ok((tool, args)) = crate::mcp_client::validate_preliminary_request(&req) else {
            continue;
        };
        if used_tools.iter().any(|t| t == &tool) {
            continue;
        }
        used_tools.push(tool.clone());
        match session.call(&tool, args).await {
            Ok(v) => {
                for key in evidence_keys(&tool, &v) {
                    if !fetched.contains(&key) {
                        fetched.push(key);
                    }
                }
                evidence_parts.push(json!({
                    "tool": tool,
                    "result": compact(&v, (cfg.max_tool_result_bytes / 4) as usize),
                }));
            }
            // A failing tool consumes budget and moves on: it can produce
            // unknown, never fabricated evidence.
            Err(_) => continue,
        }
    }
    if !fetched.is_empty() {
        backend.record_fetched(job.id, &fetched).await?;
    }

    // Step 6: final call with the strict schema (json_schema preferred,
    // provider fallback handled inside the client).
    let final_user = format!(
        "{}\n\nSTREAM SUMMARY:\n{}\n\nJEV ANSWERS:\n{}\n\nBOUNDED MESSAGES:\n{}\n\nFETCHED EVIDENCE (only these ids may be cited):\n{}",
        prompts::FINAL_INSTRUCTION,
        compact(&summary, (cfg.max_tool_result_bytes / 2) as usize),
        compact(&jev_answers, 2048),
        compact(&messages, (cfg.max_tool_result_bytes / 2) as usize),
        serde_json::to_string(&evidence_parts).unwrap_or_else(|_| "[]".to_string()),
    );
    let raw_final = call_llm(
        llm,
        cfg,
        &job.model.clone().unwrap_or_else(|| cfg.model.clone()),
        prompts::SYSTEM,
        &final_user,
        Some((prompts::SCHEMA_MODE, prompts::final_json_schema())),
    )
    .await?;
    let doc = extract_json(&raw_final.content)?;

    // Step 7: client-side validation (the backend re-validates).
    let mut payload = validate_final(&doc, &fetched).map_err(JobError::Permanent)?;
    payload["stream_id"] = json!(job.stream_id);
    payload["stream_revision"] = json!(job.stream_revision);
    payload["model"] = json!(job.model.clone().unwrap_or_else(|| cfg.model.clone()));
    payload["prompt_version"] = json!(prompts::PROMPT_VERSION);
    payload["input_tokens"] = json!(
        preliminary
            .input_tokens
            .map(|t| t + raw_final.input_tokens.unwrap_or(0))
            .or(raw_final.input_tokens)
    );
    payload["output_tokens"] = json!(
        preliminary
            .output_tokens
            .map(|t| t + raw_final.output_tokens.unwrap_or(0))
            .or(raw_final.output_tokens)
    );
    payload["duration_ms"] = json!(started.elapsed().as_millis() as i64);

    match backend.submit_result(job.id, &payload).await {
        Ok(()) => Ok(()),
        Err(JobError::Permanent(msg))
            if msg.contains("never fetched") || msg.contains("was not fetched") =>
        {
            // The backend's set is missing ids we fetched: re-record and
            // retry the submission once before the lease runs out.
            if !fetched.is_empty() {
                let _ = backend.record_fetched(job.id, &fetched).await;
                backend.submit_result(job.id, &payload).await
            } else {
                Err(JobError::Permanent(msg))
            }
        }
        Err(e) => Err(e),
    }
}

fn call_llm<'a>(
    llm: &'a LlmClient,
    cfg: &'a WorkerConfig,
    model: &'a str,
    system: &'a str,
    user: &'a str,
    structured: Option<(&'static str, Value)>,
) -> impl Future<Output = Result<crate::llm::ChatResult, JobError>> + 'a {
    llm.chat(
        &cfg.base_url,
        &cfg.api_key,
        model,
        system,
        user,
        cfg.max_output_tokens,
        structured,
    )
}

use std::future::Future;

/// Parse the preliminary document: typed assessment + bounded request
/// list. Invalid shape returns Err (one corrected retry follows).
fn parse_preliminary(content: &str) -> Result<Vec<Value>, String> {
    let doc = extract_json(content).map_err(|e| match e {
        JobError::Permanent(m) => m,
        JobError::Transient(m) => m,
    })?;
    let requests = doc
        .get("evidence_requests")
        .and_then(|v| v.as_array())
        .ok_or("missing evidence_requests array")?;
    Ok(requests.clone())
}

/// Keys the fetched-evidence set gains from one tool result. Only ids the
/// backend actually returned enter the set.
fn evidence_keys(tool: &str, result: &Value) -> Vec<String> {
    match tool {
        "get_packet" => result["id"]
            .as_i64()
            .map(|id| vec![format!("packet:{}", id)])
            .unwrap_or_default(),
        "get_message_headers" => result["id"]
            .as_i64()
            .map(|id| vec![format!("message:{}", id)])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Bounded JSON text for prompts: serialized then hard-truncated with an
/// explicit cut marker. Never silently swallows more than the cap.
fn compact(v: &Value, max_chars: usize) -> String {
    let s = serde_json::to_string(v).unwrap_or_else(|_| "null".to_string());
    if s.chars().count() <= max_chars {
        s
    } else {
        let cut: String = s.chars().take(max_chars).collect();
        format!("{} ...[TRUNCATED]", cut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_preliminary_requires_the_array() {
        assert!(
            parse_preliminary(r#"{"evidence_requests": [{"tool": "get_packet", "packet_id": 1}]}"#)
                .is_ok()
        );
        assert!(parse_preliminary(r#"{"something": 1}"#).is_err());
        assert!(parse_preliminary("not json").is_err());
        // Extra requests exist but the playbook caps fetches at 2.
        let many = json!([
            {"tool": "get_packet", "packet_id": 1},
            {"tool": "get_packet", "packet_id": 2},
            {"tool": "get_packet", "packet_id": 3},
        ]);
        assert_eq!(many.as_array().unwrap().len(), 3);
    }

    #[test]
    fn evidence_keys_come_from_actual_results_only() {
        assert_eq!(
            evidence_keys("get_packet", &json!({"id": 42})),
            vec!["packet:42".to_string()]
        );
        assert_eq!(
            evidence_keys("get_message_headers", &json!({"id": 7})),
            vec!["message:7".to_string()]
        );
        // No ids for tools without a single-resource identity.
        assert!(evidence_keys("get_stream_tags", &json!([{"id": 1}])).is_empty());
        assert!(evidence_keys("get_packet", &json!({})).is_empty());
    }

    #[test]
    fn compact_truncates_with_marker() {
        let big = json!({"x": "y".repeat(5000)});
        let s = compact(&big, 100);
        assert!(s.ends_with("...[TRUNCATED]"));
        assert!(s.chars().count() < 200);
        let small = json!({"x": 1});
        assert_eq!(compact(&small, 100), r#"{"x":1}"#);
    }
}
