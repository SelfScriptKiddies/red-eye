// Fixed prompt files (embedded; PROMPT_VERSION in the backend
// api::soc::settings must stay in sync - both are 1). Short fixed system
// prompts, explicit unknown requirement, prohibition of invented ids.

/// Backend-owned triage prompt format version. Bump ONLY with the
/// backend's soc prompt changes.
pub const PROMPT_VERSION: i32 = 1;

pub const SYSTEM: &str = "\
You are a CTF network-triage assistant. You classify one captured network \
stream. Rules you must never break:
- Base every claim ONLY on the evidence provided in this conversation.
- If the evidence is insufficient, answer verdict=unknown. Never guess.
- Never invent packet ids, message ids, headers, attacks or tool results.
- Keep the summary under 500 characters. No hidden reasoning: output the \
final JSON document only.\
";

pub const PRELIMINARY_INSTRUCTION: &str = "\
Assess the stream below and answer with ONLY a JSON object:
{\"preliminary_attack_likelihood\": <0..1>,
 \"verdict_hint\": \"benign|suspicious|likely_attack|unknown\",
 \"evidence_requests\": [<0, 1 or 2 request objects>]}
Each request object is one of:
{\"tool\": \"get_packet\", \"packet_id\": <id>}
{\"tool\": \"get_message_headers\", \"message_id\": <id>}
{\"tool\": \"get_stream_tags\", \"stream_id\": <id>}
{\"tool\": \"list_related_streams\", \"stream_id\": <id>, \"window_secs\": <1..3600>}
Only request evidence whose ids appear in the material above. If the \
material already suffices, return an empty evidence_requests list.\
";

pub const FINAL_INSTRUCTION: &str = "\
Produce the final triage answer as ONLY a JSON object matching exactly:
{\"verdict\": \"benign\" | \"suspicious\" | \"likely_attack\" | \"unknown\",
 \"confidence\": <0..1>,
 \"severity\": <0|1|2|3>,
 \"summary\": \"<concise finding, max 500 chars>\",
 \"evidence\": [<{\"packet_id\": <id>, \"explanation\": \"<why>\"} | {\"message_id\": <id>, \"explanation\": \"<why>\"}>],
 \"indicators\": [<short strings, max 20>],
 \"recommended_action\": \"none\" | \"monitor\" | \"manual_review\" | \"block_remote_ip\"}
Requirements:
- verdict=unknown and recommended_action=none when evidence is insufficient.
- Every evidence id must come from the fetched evidence in this \
conversation; cite at most 5 items.
- confidence reflects certainty; never fabricate a high confidence.\
";

/// The final-answer JSON schema sent with response_format=json_schema
/// when the provider supports it.
pub fn final_json_schema() -> serde_json::Value {
    serde_json::json!({
        "name": "soc_triage_result",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string", "enum": ["benign", "suspicious", "likely_attack", "unknown"]},
                "confidence": {"type": "number"},
                "severity": {"type": "integer"},
                "summary": {"type": "string"},
                "evidence": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "packet_id": {"type": "integer"},
                            "message_id": {"type": "integer"},
                            "explanation": {"type": "string"}
                        }
                    }
                },
                "indicators": {"type": "array", "items": {"type": "string"}},
                "recommended_action": {"type": "string", "enum": ["none", "monitor", "manual_review", "block_remote_ip"]}
            },
            "required": ["verdict", "confidence", "severity", "summary", "evidence", "indicators", "recommended_action"]
        }
    })
}

pub const JSON_MODE: &str = "json_object";
pub const SCHEMA_MODE: &str = "json_schema";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompts_match_version_contract() {
        assert_eq!(PROMPT_VERSION, 1);
        assert!(SYSTEM.contains("verdict=unknown"));
        assert!(FINAL_INSTRUCTION.contains("never fabricate"));
        // Schema enum matches the backend's accepted verdict set.
        let s = final_json_schema();
        let verdicts: Vec<&str> = s["schema"]["properties"]["verdict"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(
            verdicts,
            ["benign", "suspicious", "likely_attack", "unknown"]
        );
    }
}
