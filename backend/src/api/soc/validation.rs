use serde_json::Value;

use super::repository::{ResultRejection, ValidatedResult};

// Strict validation of worker-submitted triage results. Model output is
// untrusted text: every field is bounds-checked, every evidence id must
// have been fetched for this job, and no failure path invents a benign
// verdict. Rejections are typed so the worker can retry within its
// attempts budget.

pub const MAX_SUMMARY_CHARS: usize = 2000;
pub const MAX_EVIDENCE_ITEMS: usize = 20;
pub const MAX_INDICATORS: usize = 20;
const MAX_EXPLANATION_CHARS: usize = 500;
const MAX_INDICATOR_CHARS: usize = 200;
const VERDICTS: [&str; 4] = ["benign", "suspicious", "likely_attack", "unknown"];
const ACTIONS: [&str; 4] = ["none", "monitor", "manual_review", "block_remote_ip"];

/// Validate the raw result payload against the active job and its
/// fetched-evidence set. Returns the insertable row or the rejection.
pub fn validate_result_payload(
    job_id: i64,
    payload: &Value,
    fetched_evidence: &[String],
) -> Result<ValidatedResult, ResultRejection> {
    let obj = payload
        .as_object()
        .ok_or(mismatch("result must be a JSON object"))?;

    let stream_id = int_field(obj, "stream_id")?;
    let stream_revision = int_field(obj, "stream_revision")?;
    let model = string_field(obj, "model", 128)?;
    let prompt_version = int_field(obj, "prompt_version")?;
    let verdict = string_field(obj, "verdict", 32)?;
    if !VERDICTS.contains(&verdict.as_str()) {
        return Err(mismatch(format!(
            "verdict must be one of: {}",
            VERDICTS.join(", ")
        )));
    }
    let confidence = obj
        .get("confidence")
        .and_then(|v| v.as_f64())
        .ok_or(mismatch("confidence must be a number in [0, 1]"))?;
    if !(0.0..=1.0).contains(&confidence) || !confidence.is_finite() {
        return Err(mismatch("confidence must be a number in [0, 1]"));
    }
    let severity = obj
        .get("severity")
        .and_then(|v| v.as_i64())
        .filter(|s| (0..=3).contains(s))
        .ok_or(mismatch("severity must be an integer 0..=3"))?;
    let summary = string_field(obj, "summary", MAX_SUMMARY_CHARS)?;
    let recommended_action = string_field(obj, "recommended_action", 32)?;
    if !ACTIONS.contains(&recommended_action.as_str()) {
        return Err(mismatch(format!(
            "recommended_action must be one of: {}",
            ACTIONS.join(", ")
        )));
    }

    let evidence = validate_evidence(obj.get("evidence"), fetched_evidence)?;
    let indicators = validate_indicators(obj.get("indicators"))?;

    Ok(ValidatedResult {
        job_id,
        stream_id,
        stream_revision,
        verdict,
        confidence,
        severity: severity as i32,
        summary,
        evidence,
        indicators,
        recommended_action,
        model,
        prompt_version: prompt_version as i32,
        input_tokens: obj.get("input_tokens").and_then(|v| v.as_i64()),
        output_tokens: obj.get("output_tokens").and_then(|v| v.as_i64()),
        duration_ms: obj.get("duration_ms").and_then(|v| v.as_i64()),
    })
}

/// Each evidence item cites exactly one packet or message id with a short
/// explanation; the id must be in the job's fetched set. Invented or
/// out-of-scope ids invalidate the whole result.
fn validate_evidence(raw: Option<&Value>, fetched: &[String]) -> Result<Value, ResultRejection> {
    let items = raw
        .and_then(|v| v.as_array())
        .ok_or(mismatch("evidence must be an array"))?;
    if items.len() > MAX_EVIDENCE_ITEMS {
        return Err(mismatch(format!(
            "evidence must contain at most {} items",
            MAX_EVIDENCE_ITEMS
        )));
    }
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = item
            .as_object()
            .ok_or(mismatch("evidence items must be objects"))?;
        let packet_id = obj.get("packet_id").and_then(|v| v.as_i64());
        let message_id = obj.get("message_id").and_then(|v| v.as_i64());
        let key = match (packet_id, message_id) {
            (Some(p), None) => format!("packet:{}", p),
            (None, Some(m)) => format!("message:{}", m),
            _ => {
                return Err(mismatch(
                    "each evidence item needs exactly one of packet_id or message_id",
                ));
            }
        };
        if !fetched.contains(&key) {
            return Err(ResultRejection::Mismatch(format!(
                "evidence id {} was not fetched for this job",
                key
            )));
        }
        let explanation = string_field(obj, "explanation", MAX_EXPLANATION_CHARS)?;
        let mut entry = serde_json::Map::new();
        if let Some(p) = packet_id {
            entry.insert("packet_id".to_string(), serde_json::json!(p));
        } else {
            entry.insert("message_id".to_string(), serde_json::json!(message_id));
        }
        entry.insert("explanation".to_string(), serde_json::json!(explanation));
        out.push(Value::Object(entry));
    }
    Ok(Value::Array(out))
}

fn validate_indicators(raw: Option<&Value>) -> Result<Value, ResultRejection> {
    let items = raw
        .and_then(|v| v.as_array())
        .ok_or(mismatch("indicators must be an array of strings"))?;
    if items.len() > MAX_INDICATORS {
        return Err(mismatch(format!(
            "indicators must contain at most {} strings",
            MAX_INDICATORS
        )));
    }
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let s = item
            .as_str()
            .ok_or(mismatch("indicators must contain only strings"))?;
        if s.chars().count() > MAX_INDICATOR_CHARS {
            return Err(mismatch(format!(
                "each indicator must be at most {} characters",
                MAX_INDICATOR_CHARS
            )));
        }
        out.push(Value::String(s.to_string()));
    }
    Ok(Value::Array(out))
}

fn int_field(obj: &serde_json::Map<String, Value>, name: &str) -> Result<i64, ResultRejection> {
    obj.get(name)
        .and_then(|v| v.as_i64())
        .ok_or(mismatch(format!("{} must be an integer", name)))
}

fn string_field(
    obj: &serde_json::Map<String, Value>,
    name: &str,
    max_chars: usize,
) -> Result<String, ResultRejection> {
    let s = obj
        .get(name)
        .and_then(|v| v.as_str())
        .ok_or(mismatch(format!("{} must be a string", name)))?;
    if s.chars().count() > max_chars {
        return Err(mismatch(format!(
            "{} must be at most {} characters",
            name, max_chars
        )));
    }
    Ok(s.to_string())
}

fn mismatch(msg: impl Into<String>) -> ResultRejection {
    ResultRejection::Mismatch(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fetched() -> Vec<String> {
        vec![
            "packet:10".to_string(),
            "packet:11".to_string(),
            "message:7".to_string(),
        ]
    }

    fn payload() -> Value {
        json!({
            "stream_id": 1,
            "stream_revision": 100,
            "model": "qwen3.8-flash-next-iq3",
            "prompt_version": 1,
            "verdict": "likely_attack",
            "confidence": 0.9,
            "severity": 2,
            "summary": "sqli in login form",
            "evidence": [{"packet_id": 10, "explanation": "' OR 1=1 in user field"}],
            "indicators": ["sqli-pattern"],
            "recommended_action": "manual_review",
            "input_tokens": 1000,
            "output_tokens": 120,
            "duration_ms": 4200,
        })
    }

    #[test]
    fn valid_payload_passes_with_normalised_evidence() {
        let r = validate_result_payload(5, &payload(), &fetched()).unwrap();
        assert_eq!(r.verdict, "likely_attack");
        assert_eq!(r.severity, 2);
        assert_eq!(r.evidence.as_array().unwrap().len(), 1);
    }

    #[test]
    fn hallucinated_evidence_id_is_rejected() {
        let mut p = payload();
        p["evidence"] = json!([{"packet_id": 999, "explanation": "made up"}]);
        let err = validate_result_payload(5, &p, &fetched()).unwrap_err();
        assert_eq!(err.code(), "mismatch");
        assert!(err.message().contains("999"));
    }

    #[test]
    fn evidence_with_both_ids_is_rejected() {
        let mut p = payload();
        p["evidence"] = json!([{"packet_id": 10, "message_id": 7, "explanation": "x"}]);
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
    }

    #[test]
    fn confidence_and_severity_ranges_are_enforced() {
        let mut p = payload();
        p["confidence"] = json!(1.5);
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
        p["confidence"] = json!(0.5);
        p["severity"] = json!(4);
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
    }

    #[test]
    fn unknown_verdict_and_action_are_rejected() {
        let mut p = payload();
        p["verdict"] = json!("definitely_attack");
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
        p["verdict"] = json!("unknown");
        p["recommended_action"] = json!("nuke_from_orbit");
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
    }

    #[test]
    fn oversized_summary_indicators_and_counts_are_rejected() {
        let mut p = payload();
        p["summary"] = json!("x".repeat(2001));
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
        p["summary"] = json!("ok");
        p["indicators"] = serde_json::Value::Array((0..21).map(|_| json!("i")).collect());
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
        p["indicators"] = json!(["ok"]);
        p["evidence"] = serde_json::Value::Array(
            (0..21)
                .map(|i| json!({"packet_id": 10, "explanation": i.to_string()}))
                .collect(),
        );
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
    }

    #[test]
    fn missing_fields_are_rejected_never_defaulted() {
        let p = json!({"stream_id": 1});
        assert_eq!(
            validate_result_payload(5, &p, &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
        assert_eq!(
            validate_result_payload(5, &json!("array"), &fetched())
                .unwrap_err()
                .code(),
            "mismatch"
        );
    }

    #[test]
    fn empty_evidence_is_valid_for_unknown_results() {
        let mut p = payload();
        p["verdict"] = json!("unknown");
        p["evidence"] = json!([]);
        p["indicators"] = json!([]);
        let r = validate_result_payload(5, &p, &fetched()).unwrap();
        assert_eq!(r.verdict, "unknown");
    }
}
