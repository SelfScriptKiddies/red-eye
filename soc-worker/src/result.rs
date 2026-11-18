use serde_json::{Value, json};

// Client-side mirror of the backend's result validation. The worker
// validates BEFORE submit so a malformed model answer fails this attempt
// locally (one schema-correction retry) instead of burning a backend
// round-trip. The backend re-validates everything: this is never the
// authority.

const VERDICTS: [&str; 4] = ["benign", "suspicious", "likely_attack", "unknown"];
const ACTIONS: [&str; 4] = ["none", "monitor", "manual_review", "block_remote_ip"];
pub const MAX_EVIDENCE_ITEMS: usize = 20;
const MAX_INDICATORS: usize = 20;
const MAX_SUMMARY_CHARS: usize = 2000;

/// Strict validation of the model's final document against the fetched
/// evidence ids ("packet:123" / "message:45" strings). Returns the
/// submission payload ready for the backend, or Err with a short reason.
pub fn validate_final(doc: &Value, fetched: &[String]) -> Result<Value, String> {
    let obj = doc.as_object().ok_or("final answer is not a JSON object")?;
    let verdict = obj
        .get("verdict")
        .and_then(|v| v.as_str())
        .ok_or("missing verdict")?;
    if !VERDICTS.contains(&verdict) {
        return Err(format!("invalid verdict '{}'", verdict));
    }
    let confidence = obj
        .get("confidence")
        .and_then(|v| v.as_f64())
        .filter(|c| (0.0..=1.0).contains(c) && c.is_finite())
        .ok_or("confidence must be a number in [0, 1]")?;
    let severity = obj
        .get("severity")
        .and_then(|v| v.as_i64())
        .filter(|s| (0..=3).contains(s))
        .ok_or("severity must be an integer 0..=3")?;
    let summary = obj
        .get("summary")
        .and_then(|v| v.as_str())
        .ok_or("missing summary")?;
    if summary.chars().count() > MAX_SUMMARY_CHARS {
        return Err("summary too long".to_string());
    }
    let action = obj
        .get("recommended_action")
        .and_then(|v| v.as_str())
        .ok_or("missing recommended_action")?;
    if !ACTIONS.contains(&action) {
        return Err(format!("invalid recommended_action '{}'", action));
    }

    let empty = Vec::new();
    let evidence_raw = obj
        .get("evidence")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);
    if evidence_raw.len() > MAX_EVIDENCE_ITEMS {
        return Err(format!("evidence exceeds {} items", MAX_EVIDENCE_ITEMS));
    }
    let mut evidence = Vec::with_capacity(evidence_raw.len());
    for item in evidence_raw {
        let io = item.as_object().ok_or("evidence item is not an object")?;
        let packet_id = io.get("packet_id").and_then(|v| v.as_i64());
        let message_id = io.get("message_id").and_then(|v| v.as_i64());
        let explanation = io
            .get("explanation")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .chars()
            .take(500)
            .collect::<String>();
        let key = match (packet_id, message_id) {
            (Some(p), None) => format!("packet:{}", p),
            (None, Some(m)) => format!("message:{}", m),
            _ => {
                return Err(
                    "evidence item needs exactly one of packet_id or message_id".to_string()
                );
            }
        };
        // Hallucinated evidence ids invalidate the whole result.
        if !fetched.contains(&key) {
            return Err(format!("evidence id {} was never fetched", key));
        }
        let mut e = serde_json::Map::new();
        if let Some(p) = packet_id {
            e.insert("packet_id".to_string(), json!(p));
        } else {
            e.insert("message_id".to_string(), json!(message_id));
        }
        e.insert("explanation".to_string(), json!(explanation));
        evidence.push(Value::Object(e));
    }

    let indicators_raw = obj
        .get("indicators")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);
    if indicators_raw.len() > MAX_INDICATORS {
        return Err(format!("indicators exceed {}", MAX_INDICATORS));
    }
    let indicators: Vec<Value> = indicators_raw
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| json!(s.chars().take(200).collect::<String>()))
        .collect();

    Ok(json!({
        "verdict": verdict,
        "confidence": confidence,
        "severity": severity,
        "summary": summary,
        "evidence": evidence,
        "indicators": indicators,
        "recommended_action": action,
        "input_tokens": obj.get("input_tokens").and_then(|v| v.as_i64()),
        "output_tokens": obj.get("output_tokens").and_then(|v| v.as_i64()),
        "duration_ms": obj.get("duration_ms").and_then(|v| v.as_i64()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc() -> Value {
        json!({
            "verdict": "likely_attack",
            "confidence": 0.9,
            "severity": 2,
            "summary": "sqli",
            "evidence": [{"packet_id": 10, "explanation": "pattern"}],
            "indicators": ["sqli"],
            "recommended_action": "manual_review",
        })
    }

    #[test]
    fn valid_document_passes() {
        let fetched = vec!["packet:10".to_string()];
        assert!(validate_final(&doc(), &fetched).is_ok());
    }

    #[test]
    fn hallucinated_evidence_fails_before_submit() {
        let fetched = vec!["packet:11".to_string()];
        assert!(
            validate_final(&doc(), &fetched)
                .unwrap_err()
                .contains("never fetched")
        );
    }

    #[test]
    fn invented_fields_do_not_break_and_ranges_hold() {
        let fetched = vec!["packet:10".to_string()];
        let mut d = doc();
        d["chain_of_thought"] = json!("hidden reasoning");
        d["confidence"] = json!(1.5);
        assert!(validate_final(&d, &fetched).is_err());
        d["confidence"] = json!(0.5);
        assert!(validate_final(&d, &fetched).is_ok());
    }

    #[test]
    fn unknown_with_empty_evidence_is_valid() {
        let mut d = doc();
        d["verdict"] = json!("unknown");
        d["evidence"] = json!([]);
        d["recommended_action"] = json!("none");
        assert!(validate_final(&d, &[]).is_ok());
    }
}
