use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

// OpenRouter Decisions API contract (verified, see the JEV design spec and
// traffic-analyzer-stack/TODO.md). NOT a chat-completions endpoint: one
// request carries the stream transcript as `state` plus every typed
// question for one revision, evaluated in a single call.

pub const DECISIONS_URL: &str = "https://openrouter.ai/api/v1/systemone";
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub const NOUL_QUESTIONS: [&str; 7] = [
    "is_sql_injection",
    "is_command_injection",
    "is_path_traversal",
    "is_xss",
    "is_scanner_probe",
    "is_exfiltration",
    "is_exploit_attempt",
];

pub const ATTACK_CLASSES: [&str; 10] = [
    "sqli",
    "xss",
    "command_injection",
    "path_traversal",
    "scanner",
    "bruteforce",
    "exfil",
    "exploit_attempt",
    "benign",
    "other",
];

pub const SEVERITY_CRITERIA: [&str; 4] = [
    "benign",
    "suspicious but likely benign",
    "likely malicious",
    "active exploit of a known vulnerability",
];

const NOUL_INSTRUCTIONS: [(&str, &str); 7] = [
    (
        "is_sql_injection",
        "Does the transcript contain SQL injection: payloads that alter SQL query semantics such as quote breaks, OR 1=1, UNION SELECT, stacked queries or time-based blind injection?",
    ),
    (
        "is_command_injection",
        "Does the transcript contain operating system command injection: shell metacharacters, command substitution, pipe or semicolon chains appended to application input?",
    ),
    (
        "is_path_traversal",
        "Does the transcript contain path traversal: ../ sequences, encoded traversal, absolute-path file access or null-byte truncation targeting the filesystem?",
    ),
    (
        "is_xss",
        "Does the transcript contain cross-site scripting: script tags, event handlers, javascript: URIs or template-break payloads aimed at other users' browsers?",
    ),
    (
        "is_scanner_probe",
        "Does the transcript look like automated scanning or probing: nmap, masscan, zgrab, nuclei patterns, mass fingerprinting requests or known-vuln endpoint sweeps?",
    ),
    (
        "is_exfiltration",
        "Does the transcript contain data exfiltration: bulk reads followed by outbound copies of flag-like strings, credential dumps or archive streams leaving the service?",
    ),
    (
        "is_exploit_attempt",
        "Does the transcript contain attempts to exploit a known vulnerability: recognizable CVE-shaped payloads, deserialization gadgets, SSRF probes or authentication bypasses?",
    ),
];

const CHOICE_INSTRUCTIONS: &str = "Pick the single best-fitting class for the whole transcript.";

fn class_criterion(name: &str) -> &'static str {
    match name {
        "sqli" => "SQL injection evidence",
        "xss" => "cross-site scripting evidence",
        "command_injection" => "OS command injection evidence",
        "path_traversal" => "path traversal evidence",
        "scanner" => "automated scanning or probing",
        "bruteforce" => "credential or token brute force",
        "exfil" => "data exfiltration",
        "exploit_attempt" => "known-vulnerability exploit attempt",
        "benign" => "normal service traffic",
        "other" => "malicious or anomalous but not classifiable above",
        _ => "",
    }
}

/// The full typed question set: 7 nouls, one choice, one score - ALL in one
/// call. Order is deterministic (BTreeMap) so the same revision always
/// produces byte-identical request bodies for a given prompt version.
pub fn build_questions() -> BTreeMap<String, QuestionSpec> {
    let mut questions = BTreeMap::new();
    for (name, instructions) in NOUL_INSTRUCTIONS {
        questions.insert(
            name.to_string(),
            QuestionSpec {
                kind: "noul".to_string(),
                instructions: instructions.to_string(),
                criteria: None,
            },
        );
    }
    let mut criteria = BTreeMap::new();
    for class in ATTACK_CLASSES {
        criteria.insert(class.to_string(), class_criterion(class).to_string());
    }
    questions.insert(
        "attack_class".to_string(),
        QuestionSpec {
            kind: "choice".to_string(),
            instructions: CHOICE_INSTRUCTIONS.to_string(),
            criteria: Some(json!(criteria)),
        },
    );
    questions.insert(
        "severity".to_string(),
        QuestionSpec {
            kind: "score".to_string(),
            instructions: "Rate the whole transcript on the four-level severity rubric."
                .to_string(),
            criteria: Some(json!(SEVERITY_CRITERIA)),
        },
    );
    questions
}

#[derive(Debug, Serialize)]
pub struct QuestionSpec {
    #[serde(rename = "type")]
    pub kind: String,
    pub instructions: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub criteria: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct DecisionsRequest {
    pub model: String,
    pub state: Value,
    pub questions: BTreeMap<String, QuestionSpec>,
}

impl DecisionsRequest {
    pub fn new(model: &str, state: Value) -> Self {
        DecisionsRequest {
            model: model.to_string(),
            state,
            questions: build_questions(),
        }
    }
}

/// Wire response. Parsing is tolerant: unknown answer keys and unknown
/// response fields are ignored for forward compatibility, but every
/// REQUIRED answer must be present, typed and in range (validated in
/// super::decision, not here).
#[derive(Debug, Deserialize)]
pub struct DecisionsResponse {
    pub answers: BTreeMap<String, Value>,
    #[serde(default)]
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: Option<i64>,
    #[serde(default)]
    pub output_tokens: Option<i64>,
    #[serde(default)]
    pub cost: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum TransportError {
    /// DNS/connect/timeout/socket: retryable with backoff.
    Network(String),
    /// 429 or 5xx: retryable, honors Retry-After (seconds).
    RateLimited(u16, Option<u64>),
    /// 5xx: retryable with backoff.
    Server(u16),
    /// Other 4xx: the identical request will never succeed - no retry
    /// until content, credentials, model or prompt version changes.
    Rejected(u16, String),
    /// 2xx whose body is not valid JSON: schema mismatch, no identical
    /// retry.
    MalformedBody(String),
}

pub struct DecisionsHttpSuccess {
    pub body: Value,
}

impl std::fmt::Debug for DecisionsHttpSuccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionsHttpSuccess").finish()
    }
}

pub type TransportFuture =
    Pin<Box<dyn Future<Output = Result<DecisionsHttpSuccess, TransportError>> + Send>>;

/// The HTTP boundary, abstracted so tests inject a fake transport: the
/// scheduler logic never talks to real sockets in tests.
pub trait DecisionsTransport: Send + Sync + 'static {
    fn send_decisions<'a>(
        &'a self,
        api_key: &'a str,
        request: &'a DecisionsRequest,
    ) -> TransportFuture;
}

fn same_origin(a: &reqwest::Url, b: &reqwest::Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

/// Redirect policy: a redirect to a different origin than the ORIGINAL
/// request URL is not followed, so Authorization can never be re-sent to
/// another host. Same-origin redirects (openrouter.ai -> openrouter.ai)
/// follow normally.
pub fn no_cross_origin_redirects(
    attempt: reqwest::redirect::Attempt<'_>,
) -> reqwest::redirect::Action {
    let Some(original) = attempt.previous().first() else {
        return attempt.follow();
    };
    if same_origin(original, attempt.url()) {
        attempt.follow()
    } else {
        attempt.error("refusing to follow redirect to a different origin with Authorization header")
    }
}

pub fn build_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(no_cross_origin_redirects))
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .timeout(DEFAULT_CALL_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {}", e))
}

/// Retry-After in seconds. Only the integer-seconds form is honored; the
/// HTTP-date form falls back to the caller's default backoff, and any
/// value is capped by the caller.
pub fn retry_after_secs(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// Bounded GET for catalog endpoints (/models): 10s total budget, body
/// capped so a hostile upstream cannot balloon backend memory. The bearer
/// key never enters errors or logs.
pub async fn fetch_json_bounded(
    url: &str,
    bearer: Option<&str>,
    cap: usize,
) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client.get(url);
    if let Some(key) = bearer {
        req = req.bearer_auth(key);
    }
    let mut resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("upstream status {}", resp.status().as_u16()));
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
        if body.len() + chunk.len() > cap {
            return Err("upstream body too large".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|e| e.to_string())
}

/// Production transport: one shared client, key per request (never stored
/// in the client), Authorization only to the configured endpoint.
pub struct ReqwestTransport {
    client: reqwest::Client,
    url: String,
}

impl ReqwestTransport {
    pub fn new() -> Result<Self, String> {
        Ok(ReqwestTransport {
            client: build_client()?,
            url: DECISIONS_URL.to_string(),
        })
    }
}

/// Fallback for the (practically unreachable) case of the HTTP client
/// failing to build at startup: the scheduler stays functional but every
/// call reads as a hard rejection, instead of taking the backend down.
pub struct UnavailableTransport;

impl DecisionsTransport for UnavailableTransport {
    fn send_decisions<'a>(
        &'a self,
        _api_key: &'a str,
        _request: &'a DecisionsRequest,
    ) -> TransportFuture {
        Box::pin(async move {
            Err(TransportError::Rejected(
                0,
                "HTTP transport unavailable".to_string(),
            ))
        })
    }
}

impl DecisionsTransport for ReqwestTransport {
    fn send_decisions<'a>(
        &'a self,
        api_key: &'a str,
        request: &'a DecisionsRequest,
    ) -> TransportFuture {
        // Serialize and clone everything up front: the returned future
        // owns its data, which keeps the boxed lifetime simple and the
        // key confined to this call.
        let client = self.client.clone();
        let url = self.url.clone();
        let api_key = api_key.to_string();
        let body = match serde_json::to_string(request) {
            Ok(b) => b,
            Err(e) => {
                return Box::pin(async move {
                    Err(TransportError::MalformedBody(format!("serialize: {}", e)))
                });
            }
        };
        Box::pin(async move {
            let response = match client
                .post(&url)
                .header(AUTHORIZATION, format!("Bearer {}", api_key))
                .header(CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    if e.is_timeout() {
                        return Err(TransportError::Network("timeout".to_string()));
                    }
                    // The redirect policy aborts cross-origin hops; that
                    // must read as a hard rejection, not a retryable
                    // network hiccup.
                    if e.is_redirect() {
                        return Err(TransportError::Rejected(
                            0,
                            "cross-origin redirect refused".to_string(),
                        ));
                    }
                    return Err(TransportError::Network(e.to_string()));
                }
            };

            let status = response.status().as_u16();
            if status == 429 {
                return Err(TransportError::RateLimited(
                    status,
                    retry_after_secs(response.headers()),
                ));
            }
            if status >= 500 {
                return Err(TransportError::Server(status));
            }
            if status == 401 || status == 403 {
                return Err(TransportError::Rejected(
                    status,
                    "authentication rejected".to_string(),
                ));
            }
            if (400..500).contains(&status) {
                // Never include request content in the error: only the body
                // the PROVIDER returned (its own error text, truncated).
                let text = response.text().await.unwrap_or_default();
                return Err(TransportError::Rejected(
                    status,
                    text.chars().take(200).collect(),
                ));
            }

            let text = response
                .text()
                .await
                .map_err(|e| TransportError::Network(e.to_string()))?;
            // Parse to Value first: a non-JSON 2xx is a malformed body, not
            // a panic, and not a retried-identical request.
            let body: Value = serde_json::from_str(&text)
                .map_err(|e| TransportError::MalformedBody(format!("invalid JSON: {}", e)))?;
            Ok(DecisionsHttpSuccess { body })
        })
    }
}

pub(crate) fn parse_response(body: Value) -> Result<DecisionsResponse, String> {
    serde_json::from_value::<DecisionsResponse>(body)
        .map_err(|e| format!("response schema mismatch: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn questions_cover_every_typed_question_in_one_call() {
        let q = build_questions();
        for name in NOUL_QUESTIONS {
            let spec = q.get(name).expect("noul present");
            assert_eq!(spec.kind, "noul");
        }
        let choice = q.get("attack_class").expect("choice present");
        assert_eq!(choice.kind, "choice");
        let criteria = choice.criteria.as_ref().expect("choice criteria");
        for class in ATTACK_CLASSES {
            assert!(criteria.get(class).is_some(), "criterion {}", class);
        }
        let score = q.get("severity").expect("score present");
        assert_eq!(score.kind, "score");
        assert_eq!(q.len(), NOUL_QUESTIONS.len() + 2);
    }

    #[test]
    fn request_serializes_state_and_typed_questions() {
        let req = DecisionsRequest::new("typesafe/jev-1.13", json!({"stream": {"id": 1}}));
        let text = serde_json::to_string(&req).unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["model"], "typesafe/jev-1.13");
        assert_eq!(value["state"]["stream"]["id"], 1);
        assert_eq!(value["questions"]["is_sql_injection"]["type"], "noul");
        assert_eq!(value["questions"]["attack_class"]["type"], "choice");
        assert!(value["questions"]["attack_class"]["criteria"]["benign"].is_string());
        assert_eq!(value["questions"]["severity"]["type"], "score");
        assert_eq!(
            value["questions"]["severity"]["criteria"][3],
            "active exploit of a known vulnerability"
        );
        // Deterministic: same inputs, same bytes.
        let again = serde_json::to_string(&DecisionsRequest::new(
            "typesafe/jev-1.13",
            json!({"stream": {"id": 1}}),
        ))
        .unwrap();
        assert_eq!(text, again);
    }

    #[test]
    fn response_tolerates_unknown_fields_and_answers() {
        let body = json!({
            "id": "gen-dec-1",
            "future_field": {"x": 1},
            "answers": {
                "is_sql_injection": {"type": "noul", "noul": 0.93, "future": true},
                "attack_class": {"type": "choice", "choice": "sqli", "confidence": 0.88},
                "severity": {"type": "score", "score": 2.7, "confidence": 0.8},
                "is_brand_new_question": {"whatever": 1}
            },
            "usage": {"input_tokens": 275, "output_tokens": 20, "cost": 0.00003}
        });
        let parsed = parse_response(body).expect("parses");
        assert_eq!(parsed.usage.unwrap().cost, Some(0.00003));
        assert!(parsed.answers.contains_key("is_brand_new_question"));
    }

    #[test]
    fn response_missing_answers_field_is_an_error_not_panic() {
        let body = json!({"id": "gen-dec-1", "usage": {}});
        assert!(parse_response(body).is_err());
        assert!(parse_response(json!(null)).is_err());
        assert!(parse_response(json!("str")).is_err());
    }

    #[test]
    fn retry_after_parses_seconds_only() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(RETRY_AFTER, "17".parse().unwrap());
        assert_eq!(retry_after_secs(&headers), Some(17));
        headers.clear();
        headers.insert(RETRY_AFTER, "not-a-number".parse().unwrap());
        assert_eq!(retry_after_secs(&headers), None);
        assert_eq!(retry_after_secs(&reqwest::header::HeaderMap::new()), None);
    }

    #[test]
    fn same_origin_matches_scheme_host_port() {
        let a: reqwest::Url = "https://openrouter.ai/api/v1/systemone".parse().unwrap();
        let same: reqwest::Url = "https://openrouter.ai/other".parse().unwrap();
        let cross: reqwest::Url = "http://openrouter.ai/x".parse().unwrap();
        let cross_host: reqwest::Url = "https://evil.example/x".parse().unwrap();
        assert!(same_origin(&a, &same));
        assert!(!same_origin(&a, &cross));
        assert!(!same_origin(&a, &cross_host));
    }

    // ---- Fake-transport pipeline: request -> transport -> validate ----
    // -> decide, without any real socket.

    #[derive(Clone)]
    struct FakeTransport {
        response: Result<Value, TransportError>,
    }

    impl FakeTransport {
        fn new(response: Result<Value, TransportError>) -> Self {
            FakeTransport { response }
        }

        fn captured_request(request: &DecisionsRequest, api_key: &str) -> (String, String) {
            // What the real transport would put on the wire: the key ONLY
            // in the Authorization header, never in the serialized body.
            (
                format!("Bearer {}", api_key),
                serde_json::to_string(request).unwrap(),
            )
        }
    }

    impl DecisionsTransport for FakeTransport {
        fn send_decisions<'a>(
            &'a self,
            api_key: &'a str,
            request: &'a DecisionsRequest,
        ) -> TransportFuture {
            let (_auth, _body) = Self::captured_request(request, api_key);
            let response = self.response.clone();
            Box::pin(async move { response.map(|b| DecisionsHttpSuccess { body: b }) })
        }
    }

    fn provider_answers(noul: f64, choice: &str, confidence: f64, score: f64) -> Value {
        let mut answers = serde_json::Map::new();
        for name in NOUL_QUESTIONS {
            answers.insert(name.to_string(), json!({"type": "noul", "noul": noul}));
        }
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": choice, "confidence": confidence}),
        );
        answers.insert(
            "severity".to_string(),
            json!({"type": "score", "score": score, "confidence": 0.9}),
        );
        json!({"answers": answers, "usage": {"input_tokens": 275, "cost": 0.00003}})
    }

    #[test]
    fn fake_transport_roundtrip_every_typed_question_and_key_confidentiality() {
        let fake = FakeTransport::new(Ok(provider_answers(0.2, "benign", 0.95, 0.4)));
        let request = DecisionsRequest::new("typesafe/jev-1.13", json!({"stream": {"id": 7}}));
        let (auth, body) = FakeTransport::captured_request(&request, "sk-or-secret-key");
        assert_eq!(auth, "Bearer sk-or-secret-key");
        // The key never rides the serialized request body.
        assert!(!body.contains("sk-or-secret-key"));
        let parsed: Value = serde_json::from_str(&body).unwrap();
        for name in NOUL_QUESTIONS {
            assert_eq!(parsed["questions"][name]["type"], "noul", "{}", name);
        }
        assert_eq!(parsed["questions"]["attack_class"]["type"], "choice");
        assert_eq!(parsed["questions"]["severity"]["type"], "score");

        // The full non-DB pipeline on the canned provider response.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(fake.send_decisions("sk-or-secret-key", &request));
        let (raw, valid) =
            crate::api::ai::decision::validate_response(result.unwrap().body).unwrap();
        assert_eq!(raw.usage.unwrap().input_tokens, Some(275));
        let decision = crate::api::ai::decision::decide(&valid, 0.7);
        assert!(!decision.is_attack);
        assert_eq!(decision.attack_class, "benign");
    }

    #[tokio::test]
    async fn fake_transport_malformed_and_rejected_flow() {
        // Malformed body: schema mismatch, never a benign verdict.
        let fake = FakeTransport::new(Ok(json!({"answers": {"is_sql_injection": {"noul": 0.9}}})));
        let request = DecisionsRequest::new("typesafe/jev-1.13", json!({"state": "x"}));
        let ok = fake.send_decisions("k", &request).await;
        let body = ok.unwrap().body;
        assert!(crate::api::ai::decision::validate_response(body).is_err());

        // Provider 4xx: a hard rejection.
        let fake = FakeTransport::new(Err(TransportError::Rejected(400, "bad request".into())));
        let err = fake.send_decisions("k", &request).await.unwrap_err();
        assert!(matches!(err, TransportError::Rejected(400, _)));

        // Rate limited with Retry-After.
        let fake = FakeTransport::new(Err(TransportError::RateLimited(429, Some(9))));
        let err = fake.send_decisions("k", &request).await.unwrap_err();
        assert!(matches!(err, TransportError::RateLimited(429, Some(9))));
    }
}
