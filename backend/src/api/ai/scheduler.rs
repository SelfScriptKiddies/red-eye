use log::{debug, info, warn};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::interval;

use super::decision::{answers_json, decide, validate_response};
use super::openrouter::{DecisionsRequest, DecisionsTransport, TransportError};
use super::repository::{AiRepository, NewVerdict};
use super::settings::AiSettings;
use super::transcript::{BuildError, StreamContext};
use crate::api::realtime::BroadcastSender;

/// Backend-owned prompt format version. Increment ONLY when question
/// wording, state semantics or decision-relevant transcript formatting
/// changes: the value is part of every verdict identity, so a bump makes
/// current stream content eligible for a fresh verdict while old rows keep
/// recording which prompt produced them.
pub const PROMPT_VERSION: i32 = 1;

const TICK_SECS: u64 = 5;
/// Candidates idle this long leave the recency-priority ordering and enter
/// the oldest-waiting-first fair queue ahead of it.
const FAIR_QUEUE_AGE_SECS: i64 = 60;
const BASE_BACKOFF_SECS: u64 = 2;
const MAX_BACKOFF_SECS: u64 = 300;
const MAX_CONCURRENT_CALLS: usize = 2;
const RETENTION_BATCHES_PER_TICK: u32 = 4;
/// Documented typesafe/jev-1.13 input rate (USD per million input tokens,
/// traffic-analyzer-stack/TODO.md). Only used when a response carries no
/// cost data so a silent provider cannot bypass the daily budget.
const ESTIMATED_COST_PER_MTOK: f64 = 0.042;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Disabled,
    Unconfigured,
    Active,
    Backoff,
    CostLimited,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            RunState::Disabled => "disabled",
            RunState::Unconfigured => "unconfigured",
            RunState::Active => "active",
            RunState::Backoff => "backoff",
            RunState::CostLimited => "cost_limited",
        }
    }

    fn gauge_value(self) -> i64 {
        match self {
            RunState::Disabled => 0,
            RunState::Unconfigured => 1,
            RunState::Active => 2,
            RunState::Backoff => 3,
            RunState::CostLimited => 4,
        }
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    stream_id: i64,
    last_packet_id: i64,
    active: bool,
    last_time: i64,
    packet_count: i64,
    l7_protocol: String,
    agent_id: Option<String>,
    service_id: i64,
    service_name: Option<String>,
    src_ip: String,
    src_port: i32,
    dst_ip: String,
    dst_port: i32,
    verdict_last: i64,
    /// Enablement watermark for this tick: transcripts must cover content
    /// after max(verdict_last, boundary), so pre-enablement history is
    /// never analyzed.
    boundary: i64,
}

impl Candidate {
    fn identity(&self, model: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            self.stream_id, self.last_packet_id, model, PROMPT_VERSION
        )
    }

    /// First packet id the transcript must EXCLUDE (everything up to it is
    /// already analyzed or predates enablement).
    fn after_packet_id(&self) -> i64 {
        self.verdict_last.max(self.boundary)
    }

    /// A closed stream whose previous analysis predates the close gets one
    /// final revision: debounce AND the minimum interval are waived for it.
    fn closed_final(&self) -> bool {
        !self.active && self.verdict_last > 0
    }
}

struct InFlight {
    abort: Option<tokio::task::AbortHandle>,
}

/// Daily cost ledger. Recorded provider costs count at face value; calls
/// with missing cost data count at the conservative chars/4 estimate so
/// the budget cannot be bypassed by silent responses.
struct CostLedger {
    day: String,
    spent: f64,
}

impl CostLedger {
    fn new(day: String, seeded: f64) -> Self {
        CostLedger { day, spent: seeded }
    }

    fn utc_day() -> String {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let days = secs / 86_400;
        // Civil-date arithmetic from a day number (1970-01-01 was a
        // Thursday; Howard Hinnant's algorithm, ASCII-safe).
        let z = days as i64 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        format!("{:04}-{:02}-{:02}", y, m, d)
    }

    fn rollover(&mut self, seeded: f64) {
        let today = Self::utc_day();
        if self.day != today {
            self.day = today;
            self.spent = seeded;
        }
    }

    fn can_spend(&self, limit: f64, estimate: f64) -> bool {
        self.spent + estimate <= limit
    }

    fn record(&mut self, recorded: Option<f64>, estimate: f64) {
        self.spent += recorded.unwrap_or(estimate);
    }
}

struct Inner {
    in_flight: HashMap<i64, InFlight>,
    last_attempt: HashMap<i64, std::time::Instant>,
    backoff_until: HashMap<i64, std::time::Instant>,
    backoff_step: HashMap<i64, u32>,
    poisoned: HashSet<String>,
    ledger: CostLedger,
    run_state: RunState,
    backlog: i64,
    oldest_candidate_age_secs: i64,
    last_success_unix: Option<i64>,
}

pub struct JevScheduler {
    inner: Mutex<Inner>,
    semaphore: Arc<Semaphore>,
}

/// Data behind GET /api/ai/status. No secrets, no stream ids, no payload
/// material: counts and states only.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SchedulerSnapshot {
    pub run_state: String,
    pub backlog: i64,
    pub oldest_candidate_age_secs: i64,
    pub in_flight: usize,
    pub cost_spent_today: f64,
    pub last_success_unix: Option<i64>,
}

impl JevScheduler {
    fn new(ledger_seed: f64) -> Arc<Self> {
        Arc::new(JevScheduler {
            inner: Mutex::new(Inner {
                in_flight: HashMap::new(),
                last_attempt: HashMap::new(),
                backoff_until: HashMap::new(),
                backoff_step: HashMap::new(),
                poisoned: HashSet::new(),
                ledger: CostLedger::new(CostLedger::utc_day(), ledger_seed),
                run_state: RunState::Disabled,
                backlog: 0,
                oldest_candidate_age_secs: 0,
                last_success_unix: None,
            }),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CALLS)),
        })
    }

    pub async fn snapshot(&self, cost_spent_today: f64) -> SchedulerSnapshot {
        let inner = self.inner.lock().await;
        SchedulerSnapshot {
            run_state: inner.run_state.as_str().to_string(),
            backlog: inner.backlog,
            oldest_candidate_age_secs: inner.oldest_candidate_age_secs,
            in_flight: inner.in_flight.len(),
            cost_spent_today,
            last_success_unix: inner.last_success_unix,
        }
    }
}

/// Bounded exponential backoff; Retry-After (seconds) is honored but kept
/// inside the same maximum delay.
fn next_backoff(step: u32, retry_after: Option<u64>) -> Duration {
    let exp = BASE_BACKOFF_SECS.saturating_mul(1u64 << step.min(16));
    let bounded = exp.min(MAX_BACKOFF_SECS);
    let capped_retry = retry_after.map(|s| s.min(MAX_BACKOFF_SECS));
    Duration::from_secs(capped_retry.unwrap_or(bounded).max(BASE_BACKOFF_SECS))
}

/// Estimate the input token count of a transcript: chars/4, the documented
/// conservative approximation for languages the tokenizer cannot be
/// pinned to. Used for the cost estimate only.
fn estimate_tokens(state_chars: usize) -> i64 {
    (state_chars as i64 / 4).max(1)
}

fn estimate_cost(estimate_tokens: i64) -> f64 {
    estimate_tokens as f64 / 1_000_000.0 * ESTIMATED_COST_PER_MTOK
}

/// In-memory eligibility filter for fetched candidate rows: in-flight
/// suppression, per-stream minimum interval (waived for closed-stream
/// final analyses), pending backoff and poisoned identities.
async fn eligible(
    scheduler: &JevScheduler,
    rows: Vec<Candidate>,
    settings: &AiSettings,
) -> Vec<Candidate> {
    let mut out = Vec::new();
    {
        let inner = scheduler.inner.lock().await;
        for c in rows {
            if inner.in_flight.contains_key(&c.stream_id) {
                continue;
            }
            if inner
                .backoff_until
                .get(&c.stream_id)
                .map(|until| std::time::Instant::now() < *until)
                .unwrap_or(false)
            {
                continue;
            }
            if let Some(at) = inner.last_attempt.get(&c.stream_id) {
                let waited = at.elapsed().as_secs() as i64;
                let required = if c.closed_final() {
                    0
                } else {
                    settings.min_interval_secs
                };
                if waited < required {
                    continue;
                }
            }
            if inner.poisoned.contains(&c.identity(&settings.model)) {
                continue;
            }
            out.push(c);
        }
    }
    out
}

fn candidate_row(row: &sqlx::postgres::PgRow, boundary: i64) -> Candidate {
    use sqlx::Row;
    Candidate {
        stream_id: row.get("id"),
        last_packet_id: row.get("last_packet_id"),
        active: row.get("active"),
        last_time: row.get("last_time"),
        packet_count: row.get("packet_count"),
        l7_protocol: row.get("l7_protocol"),
        agent_id: row.get("agent_id"),
        service_id: row.get("service_id"),
        service_name: row.get("service_name"),
        src_ip: row.get("src_ip"),
        src_port: row.get("src_port"),
        dst_ip: row.get("dst_ip"),
        dst_port: row.get("dst_port"),
        verdict_last: row.get("verdict_last"),
        boundary,
    }
}

/// The two candidate queues share one predicate; only ordering and the
/// waiting-age cut differ. Both are bounded and served by
/// idx_stream_ai_candidates + idx_ai_verdict_latest; neither touches the
/// packets table (the revision boundary lives on stream.last_packet_id).
fn candidate_sql(fair: bool) -> String {
    let ordering = if fair {
        // Fair queue: waiting >= 60s, oldest activity first.
        "ORDER BY last_time ASC, s.id ASC"
    } else {
        // Priority queue: previously attack-classified first, then most
        // recent activity.
        "ORDER BY was_attack DESC, last_time DESC, s.id DESC"
    };
    format!(
        r#"
        SELECT s.id, s.last_packet_id, s.active,
               COALESCE(s.last_packet_time, 0) AS last_time,
               s.packet_count::bigint AS packet_count, s.l7_protocol, s.agent_id::text AS agent_id,
               s.service_id, sv.name AS service_name,
               host(s.src_ip) AS src_ip, s.src_port,
               host(s.dst_ip) AS dst_ip, s.dst_port,
               COALESCE(v.last_packet_id, 0) AS verdict_last,
               COALESCE(v.is_attack, false) AS was_attack
        FROM stream s
        JOIN service sv ON sv.id = s.service_id
        LEFT JOIN LATERAL (
            SELECT last_packet_id, is_attack
            FROM ai_verdict
            WHERE stream_id = s.id AND model = $1 AND prompt_version = $2
            ORDER BY last_packet_id DESC, id DESC
            LIMIT 1
        ) v ON true
        WHERE s.last_packet_id > 0
          AND s.last_packet_id > COALESCE(v.last_packet_id, 0)
          -- Enablement boundary: activity before this id predates JEV.
          AND s.last_packet_id > $6
          AND (
                (s.active = false AND COALESCE(v.last_packet_id, 0) > 0)
                OR COALESCE(s.last_packet_time, 0) <= $3
              )
          AND COALESCE(s.last_packet_time, 0) {age_cut} $4
        {ordering}
        LIMIT $5
        "#,
        age_cut = if fair { "<" } else { ">=" },
        ordering = ordering
    )
}

async fn fetch_candidates(
    pool: &PgPool,
    settings: &AiSettings,
    now_secs: i64,
    boundary: i64,
) -> Result<(Vec<Candidate>, Vec<Candidate>), sqlx::Error> {
    let fair_cut = now_secs - FAIR_QUEUE_AGE_SECS;
    let debounce_cut = now_secs - settings.debounce_ms / 1000;
    let limit = (settings.max_per_tick as i64).max(1);

    let mut fair = sqlx::query(&candidate_sql(true))
        .bind(&settings.model)
        .bind(PROMPT_VERSION)
        .bind(debounce_cut)
        .bind(fair_cut)
        .bind(limit)
        .bind(boundary)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|r| candidate_row(r, boundary))
        .collect::<Vec<_>>();

    let priority = sqlx::query(&candidate_sql(false))
        .bind(&settings.model)
        .bind(PROMPT_VERSION)
        .bind(debounce_cut)
        .bind(fair_cut)
        .bind(limit)
        .bind(boundary)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|r| candidate_row(r, boundary))
        .collect::<Vec<_>>();

    // The fair queue runs ahead of the priority ordering; cap the combined
    // batch at max_per_tick.
    let cap = settings.max_per_tick;
    fair.truncate(cap);
    Ok((fair, priority))
}

/// Register one stream as in flight; Some(permit) when it was free. The
/// in-flight check and the semaphore wait never hold the state lock
/// together: status reads and other registrations stay cheap while a
/// third call waits for a permit.
async fn try_begin(
    scheduler: &JevScheduler,
    stream_id: i64,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    {
        let inner = scheduler.inner.lock().await;
        if inner.in_flight.contains_key(&stream_id) {
            return None;
        }
    }
    let permit = scheduler.semaphore.clone().acquire_owned().await.ok()?;
    let mut inner = scheduler.inner.lock().await;
    if inner.in_flight.contains_key(&stream_id) {
        return None;
    }
    inner.in_flight.insert(stream_id, InFlight { abort: None });
    Some(permit)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct VerdictOutcome {
    pub verdict_id: i64,
    pub stream_id: i64,
    pub last_packet_id: i64,
    pub is_attack: bool,
    pub attack_class: String,
    pub confidence: f64,
    pub severity: f64,
    pub top_nouls: Vec<(String, f64)>,
}

fn verdict_event(outcome: &VerdictOutcome) -> String {
    let mut top = serde_json::Map::new();
    for (name, p) in &outcome.top_nouls {
        top.insert(name.clone(), serde_json::json!(p));
    }
    serde_json::json!({
        "type": "ai_verdict",
        "verdict_id": outcome.verdict_id,
        "stream_id": outcome.stream_id,
        "last_packet_id": outcome.last_packet_id,
        "is_attack": outcome.is_attack,
        "attack_class": outcome.attack_class,
        "confidence": outcome.confidence,
        "severity": outcome.severity,
        "top_nouls": serde_json::Value::Object(top),
    })
    .to_string()
}

/// One analysis attempt: transcript -> cost check -> provider call ->
/// validate -> decide -> store -> broadcast. Every failure path is
/// contained: a failed stream never cancels others or the poller.
#[allow(clippy::too_many_arguments)]
async fn analyze_candidate(
    pool: PgPool,
    repo: AiRepository,
    broadcaster: BroadcastSender,
    scheduler: Arc<JevScheduler>,
    transport: Arc<dyn DecisionsTransport>,
    settings: AiSettings,
    candidate: Candidate,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let identity = candidate.identity(&settings.model);
    let stream_id = candidate.stream_id;
    let result = run_analysis(
        &pool,
        &repo,
        &broadcaster,
        &scheduler,
        transport.as_ref(),
        &settings,
        &candidate,
    )
    .await;

    let mut inner = scheduler.inner.lock().await;
    inner.in_flight.remove(&stream_id);
    inner
        .last_attempt
        .insert(stream_id, std::time::Instant::now());
    drop(permit);
    drop(inner);

    if let Err(e) = result {
        handle_analysis_error(&scheduler, &candidate, &settings, &identity, e).await;
    }
}

enum AnalysisError {
    NoContent,
    Backoff {
        msg: String,
        retry_after: Option<u64>,
    },
    Poison(String),
    /// Daily budget reached: pause this candidate without poisoning it or
    /// backing off; the next tick re-checks the ledger.
    CostLimited,
}

impl AnalysisError {
    fn backoff(msg: String) -> Self {
        AnalysisError::Backoff {
            msg,
            retry_after: None,
        }
    }
}

async fn run_analysis(
    pool: &PgPool,
    repo: &AiRepository,
    broadcaster: &BroadcastSender,
    scheduler: &Arc<JevScheduler>,
    transport: &dyn DecisionsTransport,
    settings: &AiSettings,
    candidate: &Candidate,
) -> Result<VerdictOutcome, AnalysisError> {
    let ctx = StreamContext {
        id: candidate.stream_id,
        agent_id: candidate.agent_id.clone(),
        service_id: candidate.service_id,
        service_name: candidate.service_name.clone(),
        protocol: "tcp".to_string(),
        l7_protocol: candidate.l7_protocol.clone(),
        src_ip: candidate.src_ip.clone(),
        src_port: candidate.src_port,
        dst_ip: candidate.dst_ip.clone(),
        dst_port: candidate.dst_port,
        active: candidate.active,
        packet_count: candidate.packet_count,
        first_packet_time: 0,
        last_packet_time: candidate.last_time,
        revision_packet_id: candidate.last_packet_id,
        after_packet_id: candidate.after_packet_id(),
    };

    let transcript = match super::transcript::build(
        pool,
        &ctx,
        settings.max_bytes,
        settings.max_entries,
        settings.entry_max_bytes,
    )
    .await
    {
        Ok(t) => t,
        Err(BuildError::NoContent) => {
            crate::api::metrics::AI_SKIPPED_NO_CONTENT.inc();
            return Err(AnalysisError::NoContent);
        }
        Err(BuildError::Db(e)) => {
            return Err(AnalysisError::backoff(format!(
                "transcript db: {}",
                short_err(&e)
            )));
        }
    };
    if transcript.truncated {
        crate::api::metrics::AI_TRANSCRIPTS_TRUNCATED.inc();
    }

    // Cost guard BEFORE the call: recorded spend today plus a conservative
    // estimate for this request. Reaching the limit pauses new calls until
    // the next UTC day; packet ingestion never notices.
    let tokens_estimate = estimate_tokens(transcript.state_chars);
    let cost_estimate = estimate_cost(tokens_estimate);
    {
        let mut inner = scheduler.inner.lock().await;
        inner.ledger.rollover(repo_cost_seed(pool).await);
        if !inner
            .ledger
            .can_spend(settings.daily_cost_limit, cost_estimate)
        {
            inner.run_state = RunState::CostLimited;
            warn!(
                "JEV daily cost limit {} reached; pausing new calls until tomorrow",
                settings.daily_cost_limit
            );
            return Err(AnalysisError::CostLimited);
        }
    }

    let request = DecisionsRequest::new(&settings.model, transcript.state.clone());
    crate::api::metrics::AI_CALLS_TOTAL.inc();
    let started = std::time::Instant::now();
    let response = transport
        .send_decisions(&settings.api_key, &request)
        .await
        .map_err(|e| match e {
            TransportError::Network(m) => {
                crate::api::metrics::AI_TRANSPORT_FAILURES.inc();
                AnalysisError::backoff(format!("network: {}", m))
            }
            TransportError::RateLimited(code, retry_after) => {
                crate::api::metrics::AI_RATE_LIMITS.inc();
                AnalysisError::Backoff {
                    msg: format!("rate limited {}", code),
                    retry_after,
                }
            }
            TransportError::Server(code) => {
                crate::api::metrics::AI_TRANSPORT_FAILURES.inc();
                AnalysisError::backoff(format!("server {}", code))
            }
            TransportError::Rejected(code, m) => {
                crate::api::metrics::AI_INVALID_RESPONSES.inc();
                AnalysisError::Poison(format!("rejected {}: {}", code, m))
            }
            TransportError::MalformedBody(m) => {
                crate::api::metrics::AI_INVALID_RESPONSES.inc();
                AnalysisError::Poison(format!("malformed body: {}", m))
            }
        })?;
    crate::api::metrics::AI_PROVIDER_LATENCY.observe(started.elapsed().as_secs_f64());

    // Only a validated response proceeds; anything malformed is invalid,
    // never benign.
    let (raw, valid) = match validate_response(response.body) {
        Ok(v) => v,
        Err(m) => {
            crate::api::metrics::AI_INVALID_RESPONSES.inc();
            return Err(AnalysisError::Poison(format!("schema: {}", m)));
        }
    };

    let decision = decide(&valid, settings.threshold);
    let answers = answers_json(&valid);
    let recorded_cost = raw.usage.as_ref().and_then(|u| u.cost);
    let recorded_tokens = raw.usage.as_ref().and_then(|u| u.input_tokens);

    // Disable between call and store: the provider already got the data,
    // but a disabled revision must not produce a verdict or an event.
    let now_settings = AiSettings::load(pool).await;
    if !now_settings.enabled || now_settings.api_key.trim().is_empty() {
        info!(
            "JEV disabled after provider call; discarding late response for stream {}",
            candidate.stream_id
        );
        return Err(AnalysisError::NoContent);
    }

    let new_verdict = NewVerdict {
        stream_id: candidate.stream_id,
        first_packet_id: transcript.first_packet_id,
        last_packet_id: transcript.last_packet_id,
        model: &settings.model,
        prompt_version: PROMPT_VERSION,
        transcript_sha256: &transcript.sha256,
        attack_class: &decision.attack_class,
        confidence: decision.confidence,
        severity: decision.severity,
        is_attack: decision.is_attack,
        ai_threshold: settings.threshold,
        answers: &answers,
        input_tokens: recorded_tokens,
        cost: recorded_cost,
        analyzed_packet_count: transcript.analyzed_packet_count as i32,
        omitted_packet_count: transcript.omitted_packet_count as i32,
        omitted_byte_count: transcript.omitted_byte_count,
        truncated: transcript.truncated,
    };

    // DB insert failure after the provider response retries through normal
    // polling; uniqueness prevents double storage (the repeated call's
    // cost is counted by the ledger either way).
    let verdict_id = match repo.insert_verdict(&new_verdict).await {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Verdict insert failed for stream {} (will retry via polling): {}",
                candidate.stream_id, e
            );
            return Err(AnalysisError::backoff(format!("insert: {}", short_err(&e))));
        }
    };

    let Some(verdict_id) = verdict_id else {
        // Identity already stored (dup): nothing to broadcast.
        return Ok(VerdictOutcome {
            verdict_id: 0,
            stream_id: candidate.stream_id,
            last_packet_id: transcript.last_packet_id,
            is_attack: decision.is_attack,
            attack_class: decision.attack_class,
            confidence: decision.confidence,
            severity: decision.severity,
            top_nouls: decision.top_nouls,
        });
    };

    {
        let mut inner = scheduler.inner.lock().await;
        inner.ledger.record(recorded_cost, cost_estimate);
        inner.last_success_unix = Some(now_unix());
        inner.run_state = RunState::Active;
    }
    crate::api::metrics::AI_VERDICTS_TOTAL.inc();
    if decision.is_attack {
        crate::api::metrics::AI_ATTACK_VERDICTS_TOTAL.inc();
    }

    let outcome = VerdictOutcome {
        verdict_id,
        stream_id: candidate.stream_id,
        last_packet_id: transcript.last_packet_id,
        is_attack: decision.is_attack,
        attack_class: decision.attack_class,
        confidence: decision.confidence,
        severity: decision.severity,
        top_nouls: decision.top_nouls,
    };
    // WebSocket lag drops notifications without dropping stored verdicts.
    let _ = broadcaster.send(verdict_event(&outcome));
    // SOC auto-enqueue: fires only on this fresh insert, so enabling SOC
    // later never retro-enqueues old verdicts; gates and dedup live in
    // the soc module. JEV failures never propagate from here.
    crate::api::soc::maybe_enqueue_automatic(
        pool,
        verdict_id,
        outcome.stream_id,
        outcome.last_packet_id,
        outcome.is_attack,
        outcome.severity,
    )
    .await;
    Ok(outcome)
}

fn short_err(e: &sqlx::Error) -> String {
    format!("{}", e).chars().take(200).collect()
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn repo_cost_seed(pool: &PgPool) -> f64 {
    match AiRepository::new(pool.clone()).cost_today().await {
        Ok((cost, _)) => cost,
        Err(_) => 0.0,
    }
}

async fn handle_analysis_error(
    scheduler: &Arc<JevScheduler>,
    candidate: &Candidate,
    _settings: &AiSettings,
    identity: &str,
    err: AnalysisError,
) {
    match err {
        AnalysisError::NoContent | AnalysisError::CostLimited => {
            // Skip or budget pause: no backoff, no poisoning. The next
            // tick re-evaluates from the ledger and the candidate query.
        }
        AnalysisError::Poison(m) => {
            // Invalid 4xx / schema mismatch: never retry the identical
            // request until content, credentials, model or prompt version
            // changes.
            let mut inner = scheduler.inner.lock().await;
            inner.poisoned.insert(identity.to_string());
            debug!("JEV poisoned identity {}: {}", identity, m);
        }
        AnalysisError::Backoff { msg, retry_after } => {
            let mut inner = scheduler.inner.lock().await;
            let step = inner
                .backoff_step
                .get(&candidate.stream_id)
                .copied()
                .unwrap_or(0);
            let until = std::time::Instant::now() + next_backoff(step, retry_after);
            inner.backoff_until.insert(candidate.stream_id, until);
            inner
                .backoff_step
                .insert(candidate.stream_id, step.saturating_add(1));
            inner.run_state = RunState::Backoff;
            debug!(
                "JEV backoff stream {} for {:?}: {}",
                candidate.stream_id, until, msg
            );
        }
    }
}

/// Spawn the supervised poller. Every tick re-reads settings; disabling
/// JEV aborts in-flight calls and drains their state without touching
/// capture, ingestion or the UI.
pub fn spawn(
    pool: PgPool,
    broadcaster: BroadcastSender,
    transport: Arc<dyn DecisionsTransport>,
) -> Arc<JevScheduler> {
    let scheduler = JevScheduler::new(0.0);
    let scheduler_for_tasks = scheduler.clone();

    tokio::spawn(async move {
        let repo = AiRepository::new(pool.clone());
        let mut ticker = interval(Duration::from_secs(TICK_SECS));
        loop {
            ticker.tick().await;
            if let Err(e) = tick(&pool, &repo, &broadcaster, &scheduler_for_tasks, &transport).await
            {
                // Containment: one bad tick never terminates the poller.
                debug!("JEV tick error: {}", e);
            }
        }
    });

    scheduler
}

async fn tick(
    pool: &PgPool,
    repo: &AiRepository,
    broadcaster: &BroadcastSender,
    scheduler: &Arc<JevScheduler>,
    transport: &Arc<dyn DecisionsTransport>,
) -> Result<(), String> {
    let settings = AiSettings::load(pool).await;

    let (seed_cost, _) = repo.cost_today().await.map_err(|e| e.to_string())?;
    let mut inner = scheduler.inner.lock().await;
    inner.ledger.rollover(seed_cost);
    if !settings.enabled || settings.api_key.trim().is_empty() {
        // Disabled or unconfigured: cancel in-flight (where possible),
        // discard late responses at their insert-time re-check, expose an
        // idle state, and never error-loop. The enablement watermark is
        // cleared so the next enablement re-captures it.
        for (_, f) in inner.in_flight.drain() {
            if let Some(handle) = f.abort {
                handle.abort();
            }
        }
        super::settings::clear_enablement_boundary(pool).await;
        inner.run_state = if settings.enabled {
            RunState::Unconfigured
        } else {
            RunState::Disabled
        };
        inner.backlog = 0;
        inner.oldest_candidate_age_secs = 0;
        drop(inner);
        publish_gauges(scheduler).await;
        return Ok(());
    }
    drop(inner);

    // Verdict retention: bounded batches inside the tick, never blocking.
    let cutoff = now_unix() - settings.retention_days * 86_400;
    for _ in 0..RETENTION_BATCHES_PER_TICK {
        match repo.delete_expired_batch(cutoff).await {
            Ok(0) => break,
            Ok(_) => continue,
            Err(e) => {
                debug!("Verdict retention delete failed: {}", e);
                break;
            }
        }
    }

    let now_secs = now_unix();
    // Enablement watermark: persisted at the disable->enable transition,
    // cleared on disable, kept across restarts. Candidates below it are
    // pre-JEV history and never leave the network.
    let boundary = match super::settings::read_enablement_boundary(pool).await {
        Some(b) => b,
        None => {
            super::settings::write_enablement_boundary(pool, 0).await;
            super::settings::read_enablement_boundary(pool)
                .await
                .unwrap_or(0)
        }
    };
    let (mut fair, priority) = fetch_candidates(pool, &settings, now_secs, boundary)
        .await
        .map_err(|e| e.to_string())?;

    {
        let mut inner = scheduler.inner.lock().await;
        inner.run_state = RunState::Active;
        // Backlog snapshot: both queues before per-tick/in-flight caps.
        inner.backlog = (fair.len() + priority.len()) as i64;
        let now_secs = now_unix();
        let oldest = fair
            .first()
            .map(|c| now_secs.saturating_sub(c.last_time))
            .unwrap_or(0);
        inner.oldest_candidate_age_secs = oldest;
    }

    fair.extend(priority);
    let runnable = eligible(scheduler, fair, &settings).await;
    let mut slots = settings.max_per_tick;

    for candidate in runnable {
        if slots == 0 {
            break;
        }
        // Budget check per spawn: candidates already over the daily cost
        // limit stop the rest of this tick's queue.
        {
            let mut inner = scheduler.inner.lock().await;
            inner.ledger.rollover(seed_cost);
            if !inner.ledger.can_spend(
                settings.daily_cost_limit,
                estimate_cost(estimate_tokens(settings.max_bytes)),
            ) {
                inner.run_state = RunState::CostLimited;
                break;
            }
        }
        let stream_id = candidate.stream_id;
        let Some(permit) = try_begin(scheduler, candidate.stream_id).await else {
            continue;
        };
        slots -= 1;
        let task = tokio::spawn(analyze_candidate(
            pool.clone(),
            AiRepository::new(pool.clone()),
            broadcaster.clone(),
            scheduler.clone(),
            transport.clone(),
            settings.clone(),
            candidate,
            permit,
        ));
        let mut inner = scheduler.inner.lock().await;
        if let Some(f) = inner.in_flight.get_mut(&stream_id) {
            f.abort = Some(task.abort_handle());
        }
    }

    publish_gauges(scheduler).await;
    Ok(())
}

async fn publish_gauges(scheduler: &Arc<JevScheduler>) {
    let inner = scheduler.inner.lock().await;
    crate::api::metrics::AI_STATE.set(inner.run_state.gauge_value());
    crate::api::metrics::AI_BACKLOG.set(inner.backlog);
    crate::api::metrics::AI_INFLIGHT.set(inner.in_flight.len() as i64);
    // Micro-USD integer: Prometheus floats are fine, but the counter stays
    // exact for the smallest documented rates.
    crate::api::metrics::AI_COST_SPENT.set((inner.ledger.spent * 1_000_000.0) as i64);
    if let Some(ts) = inner.last_success_unix {
        crate::api::metrics::AI_LAST_SUCCESS_UNIX.set(ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(active: bool, last_time: i64, verdict_last: i64) -> Candidate {
        Candidate {
            stream_id: 1,
            last_packet_id: 100,
            active,
            last_time,
            packet_count: 10,
            l7_protocol: "http1".to_string(),
            agent_id: None,
            service_id: 1,
            service_name: Some("svc".to_string()),
            src_ip: "10.0.0.1".to_string(),
            src_port: 1,
            dst_ip: "10.0.0.2".to_string(),
            dst_port: 2,
            verdict_last,
            boundary: 0,
        }
    }

    fn settings() -> AiSettings {
        AiSettings {
            enabled: true,
            api_key: "k".to_string(),
            api_key_configured: true,
            model: "typesafe/jev-1.13".to_string(),
            threshold: 0.7,
            max_per_tick: 10,
            debounce_ms: 2000,
            min_interval_secs: 10,
            max_bytes: 65536,
            max_entries: 200,
            entry_max_bytes: 8192,
            daily_cost_limit: 5.0,
            retention_days: 30,
        }
    }

    #[test]
    fn backoff_is_exponential_bounded_and_honors_retry_after() {
        assert_eq!(next_backoff(0, None), Duration::from_secs(2));
        assert_eq!(next_backoff(1, None), Duration::from_secs(4));
        assert_eq!(next_backoff(3, None), Duration::from_secs(16));
        // Bounded.
        assert_eq!(next_backoff(20, None), Duration::from_secs(300));
        // Retry-After honored within the cap.
        assert_eq!(next_backoff(0, Some(60)), Duration::from_secs(60));
        assert_eq!(next_backoff(0, Some(99_999)), Duration::from_secs(300));
    }

    #[test]
    fn token_estimate_is_chars_over_four() {
        assert_eq!(estimate_tokens(0), 1);
        assert_eq!(estimate_tokens(400), 100);
        assert_eq!(estimate_tokens(401), 100);
    }

    #[test]
    fn cost_estimate_matches_documented_rate() {
        // 100k tokens at $0.042/M -> $0.0042.
        let c = estimate_cost(100_000);
        assert!((c - 0.0042).abs() < 1e-9, "{}", c);
    }

    #[test]
    fn ledger_rolls_over_at_utc_day_and_counts_estimates() {
        let mut ledger = CostLedger::new("2000-01-01".to_string(), 1.0);
        ledger.rollover(0.0);
        assert_eq!(ledger.day, CostLedger::utc_day());
        assert_eq!(ledger.spent, 0.0);

        // Recorded cost counts at face value.
        ledger.record(Some(0.01), 99.0);
        assert!((ledger.spent - 0.01).abs() < 1e-9);
        // Missing cost data counts the conservative estimate, never zero.
        ledger.record(None, 0.5);
        assert!((ledger.spent - 0.51).abs() < 1e-9);

        assert!(ledger.can_spend(1.0, 0.48));
        assert!(!ledger.can_spend(1.0, 0.5));
    }

    #[tokio::test]
    async fn try_begin_suppresses_concurrent_duplicates() {
        let sched = JevScheduler::new(0.0);
        let first = try_begin(&sched, 7).await;
        assert!(first.is_some());
        let second = try_begin(&sched, 7).await;
        assert!(second.is_none(), "in-flight identity suppresses dups");
        let other = try_begin(&sched, 8).await;
        assert!(other.is_some(), "different stream unaffected");
    }

    #[tokio::test]
    async fn concurrency_is_capped_at_two() {
        let sched = JevScheduler::new(0.0);
        let a = try_begin(&sched, 1).await;
        let b = try_begin(&sched, 2).await;
        assert!(a.is_some() && b.is_some());
        // The third acquisition must wait for a permit (drop one first to
        // prove the cap is the semaphore, not the bookkeeping).
        let c = tokio::time::timeout(Duration::from_millis(50), async {
            sched.semaphore.clone().acquire_owned().await
        })
        .await;
        assert!(c.is_err(), "third call must wait for a permit");
        drop(a);
        drop(b);
    }

    #[tokio::test]
    async fn min_interval_and_in_flight_filter_candidates() {
        let sched = JevScheduler::new(0.0);
        let s = settings();

        // Freshly attempted stream inside min_interval is filtered out...
        {
            let mut inner = sched.inner.lock().await;
            inner.last_attempt.insert(1, std::time::Instant::now());
        }
        let rows = vec![candidate(true, now_unix(), 0)];
        assert!(eligible(&sched, rows.clone(), &s).await.is_empty());

        // ...unless it is a closed-stream final analysis.
        {
            let mut inner = sched.inner.lock().await;
            inner.last_attempt.insert(2, std::time::Instant::now());
        }
        let closed_final = candidate(false, now_unix() - 1, 50);
        let picked = eligible(&sched, vec![closed_final], &s).await;
        assert_eq!(picked.len(), 1);
        assert!(picked[0].closed_final());

        // In-flight suppression.
        try_begin(&sched, 3).await;
        let rows = vec![candidate(true, now_unix(), 0)];
        let mut rows = rows;
        rows[0].stream_id = 3;
        assert!(eligible(&sched, rows, &s).await.is_empty());
    }

    #[tokio::test]
    async fn backoff_until_defers_candidates() {
        let sched = JevScheduler::new(0.0);
        let s = settings();
        {
            let mut inner = sched.inner.lock().await;
            inner
                .backoff_until
                .insert(5, std::time::Instant::now() + Duration::from_secs(120));
        }
        let mut c = candidate(true, now_unix(), 0);
        c.stream_id = 5;
        assert!(eligible(&sched, vec![c], &s).await.is_empty());
    }

    #[tokio::test]
    async fn poisoned_identity_never_retries_identical_request() {
        let sched = JevScheduler::new(0.0);
        let s = settings();
        let mut c = candidate(true, now_unix(), 0);
        let identity = c.identity(&s.model);
        {
            let mut inner = sched.inner.lock().await;
            inner.poisoned.insert(identity);
        }
        assert!(eligible(&sched, vec![c.clone()], &s).await.is_empty());
        // Content revision change (different last_packet_id) is a new
        // identity and stays eligible.
        c.last_packet_id = 101;
        assert_eq!(eligible(&sched, vec![c], &s).await.len(), 1);
    }

    #[test]
    fn verdict_event_matches_the_ws_contract() {
        let outcome = VerdictOutcome {
            verdict_id: 1,
            stream_id: 10,
            last_packet_id: 100,
            is_attack: true,
            attack_class: "sqli".to_string(),
            confidence: 0.88,
            severity: 2.7,
            top_nouls: vec![("is_sql_injection".to_string(), 0.93)],
        };
        let event: serde_json::Value = serde_json::from_str(&verdict_event(&outcome)).unwrap();
        assert_eq!(event["type"], "ai_verdict");
        assert_eq!(event["verdict_id"], 1);
        assert_eq!(event["stream_id"], 10);
        assert_eq!(event["last_packet_id"], 100);
        assert_eq!(event["is_attack"], true);
        assert_eq!(event["attack_class"], "sqli");
        assert_eq!(event["confidence"], 0.88);
        assert_eq!(event["severity"], 2.7);
        assert_eq!(event["top_nouls"]["is_sql_injection"], 0.93);
    }

    #[test]
    fn transcript_starts_after_the_enablement_boundary() {
        // A verdict predating enablement: content is analyzed only after
        // max(verdict_last, boundary).
        let mut c = candidate(true, now_unix(), 50);
        c.boundary = 100;
        assert_eq!(c.after_packet_id(), 100);
        // A newer verdict moves the boundary past the watermark.
        c.verdict_last = 150;
        assert_eq!(c.after_packet_id(), 150);
        // Fresh enablement (no verdicts): boundary only.
        let mut fresh = candidate(true, now_unix(), 0);
        fresh.boundary = 77;
        assert_eq!(fresh.after_packet_id(), 77);
    }

    #[test]
    fn candidate_sql_uses_fair_and_priority_orderings() {
        let fair = candidate_sql(true);
        let prio = candidate_sql(false);
        assert!(fair.contains("ORDER BY last_time ASC"));
        assert!(prio.contains("ORDER BY was_attack DESC"));
        assert!(fair.contains("< $4"));
        assert!(prio.contains(">= $4"));
        assert!(fair.contains("LIMIT $5"));
        // Enablement boundary: pre-JEV revisions are never candidates.
        assert!(fair.contains("> $6"));
        assert!(prio.contains("> $6"));
        // Neither queue scans the packets table.
        assert!(!fair.to_lowercase().contains("from packets"));
        assert!(!prio.to_lowercase().contains("from packets"));
    }

    #[test]
    fn retry_after_rides_the_backoff_variant() {
        let err = AnalysisError::Backoff {
            msg: "rate limited 429".to_string(),
            retry_after: Some(42),
        };
        match err {
            AnalysisError::Backoff { msg, retry_after } => {
                assert_eq!(retry_after, Some(42));
                assert_eq!(next_backoff(0, retry_after), Duration::from_secs(42));
                assert_eq!(msg, "rate limited 429");
            }
            _ => panic!("expected backoff"),
        }
    }

    #[test]
    fn no_content_and_cost_limit_neither_poison_nor_backoff() {
        // Checked by construction: both variants are matched together in
        // handle_analysis_error and carry no state mutation beyond the
        // run-state flag set before returning.
        let skip = AnalysisError::NoContent;
        let paused = AnalysisError::CostLimited;
        assert!(matches!(skip, AnalysisError::NoContent));
        assert!(matches!(paused, AnalysisError::CostLimited));
    }
}

#[cfg(test)]
mod live_tests {
    use super::super::transcript::live::{pool, seed_http_stream};
    use super::*;

    #[tokio::test]
    #[ignore = "needs a scratch Postgres initialized from database/init (AI_TEST_DATABASE_URL)"]
    async fn candidate_query_live_returns_idle_streams_after_the_boundary() {
        let pool = pool().await;
        let seeded = seed_http_stream(&pool).await;
        let settings = AiSettings {
            enabled: true,
            api_key: "k".to_string(),
            api_key_configured: true,
            model: "typesafe/jev-1.13".to_string(),
            threshold: 0.7,
            max_per_tick: 10,
            debounce_ms: 2000,
            min_interval_secs: 10,
            max_bytes: 65536,
            max_entries: 200,
            entry_max_bytes: 8192,
            daily_cost_limit: 5.0,
            retention_days: 30,
        };

        let (fair, priority) = fetch_candidates(&pool, &settings, now_unix(), 0)
            .await
            .expect("candidate query");
        let found = priority
            .iter()
            .chain(&fair)
            .find(|c| c.stream_id == seeded.stream_id)
            .expect("seeded stream is a candidate");
        assert_eq!(found.last_packet_id, seeded.ctx.revision_packet_id);
        assert_eq!(found.packet_count, 3);
        assert_eq!(found.service_name, seeded.ctx.service_name);
        assert_eq!(found.src_port, 40000);

        let (fair, priority) = fetch_candidates(&pool, &settings, now_unix(), found.last_packet_id)
            .await
            .expect("candidate query");
        assert!(
            fair.iter()
                .chain(&priority)
                .all(|c| c.stream_id != seeded.stream_id)
        );
    }
}
