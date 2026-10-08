use log::{debug, error, info, warn};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::api::settings::get_setting;

/// IP → team name mapping fetched from the farm.
/// Refreshed on cache miss or every 5 minutes.
struct TeamCache {
    map: HashMap<String, String>,
    last_refresh: std::time::Instant,
}

impl TeamCache {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            last_refresh: std::time::Instant::now() - std::time::Duration::from_secs(600),
        }
    }

    fn is_stale(&self) -> bool {
        self.last_refresh.elapsed() > std::time::Duration::from_secs(300)
    }
}

/// Dedup + retry queue for flag submissions.
struct FlagQueue {
    submitted: HashSet<String>,
    submitted_order: VecDeque<String>,
    retry_queue: VecDeque<FlagEntry>,
}

struct FlagEntry {
    flag: String,
    team: String,
    attempts: u8,
}

impl FlagQueue {
    fn new() -> Self {
        Self {
            submitted: HashSet::new(),
            submitted_order: VecDeque::new(),
            retry_queue: VecDeque::new(),
        }
    }

    fn mark_submitted(&mut self, flag: String) {
        if self.submitted.insert(flag.clone()) {
            self.submitted_order.push_back(flag);
        }
        while self.submitted.len() > MAX_SUBMITTED_CACHE {
            if let Some(oldest) = self.submitted_order.pop_front() {
                self.submitted.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn is_known(&self, flag: &str) -> bool {
        self.submitted.contains(flag) || self.retry_queue.iter().any(|e| e.flag == flag)
    }
}

const MAX_RETRY_ATTEMPTS: u8 = 3;
const MAX_SUBMITTED_CACHE: usize = 10000;

/// Fetch team mapping from S4DFarm's /api/get_config endpoint.
/// Returns IP→team_name map built from the TEAMS config dict (name→ip inverted).
async fn fetch_team_map(farm_url: &str, farm_password: &str) -> Option<HashMap<String, String>> {
    let base = farm_url.trim_end_matches("/api/post_flags").trim_end_matches('/');
    let config_url = format!("{}/api/get_config", base);

    let client = reqwest::Client::new();
    let mut req = client.get(&config_url)
        .timeout(std::time::Duration::from_secs(5));

    if !farm_password.is_empty() {
        req = req.header("Authorization", farm_password);
    }

    let resp = req.send().await.ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;

    let teams = body.get("TEAMS")?.as_object()?;
    let mut map = HashMap::new();
    for (name, ip_val) in teams {
        if let Some(ip) = ip_val.as_str() {
            map.insert(ip.to_string(), name.clone());
        }
    }

    info!("Fetched {} teams from farm", map.len());
    Some(map)
}

/// Background task: polls for new triggers with special_type (flag, alert).
/// Runs every 3 seconds.
pub fn spawn_special_trigger_handler(
    pool: PgPool,
    broadcaster: Arc<broadcast::Sender<String>>,
) {
    let team_cache = Arc::new(RwLock::new(TeamCache::new()));
    let flag_queue = Arc::new(RwLock::new(FlagQueue::new()));

    tokio::spawn(async move {
        use tokio::time::{interval, Duration};
        let mut ticker = interval(Duration::from_secs(3));
        let mut last_check_id: i32 = 0;

        loop {
            ticker.tick().await;

            // Find triggers with special_type since last check (deduplicated per packet+tag)
            //
            // Direction guard (flags only): a flag tag configured 'in'/'out'
            // must only be submitted when the packet actually flowed that way
            // (packets.direction: 'dest' = inbound to the machine, 'orig' =
            // outbound). Defense-in-depth against analyzers that predate
            // direction-aware filtering. Alerts stay direction-agnostic —
            // they are informational (WebSocket) and useful in both
            // directions. Unknown/NULL/'both' direction values are treated
            // as Both (mirrors the analyzer DTO mapping).
            let triggers = match sqlx::query_as::<_, (i32, i32, i32, i32, String, String, i32)>(
                "SELECT DISTINCT ON (t.packet_id, tn.id)
                        t.packet_id, t.start_index, t.end_index, tn.id as tag_id,
                        tn.name as tag_name, tn.special_type, p.stream_id
                 FROM tag t
                 JOIN tag_name tn ON tn.id = t.tag_id
                 JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
                 WHERE tn.special_type IS NOT NULL
                   AND t.packet_id > $1
                   AND (
                        tn.special_type <> 'flag'
                        OR tn.direction IS NULL
                        OR tn.direction NOT IN ('in', 'out')
                        OR (tn.direction = 'in'  AND p.direction = 'dest')
                        OR (tn.direction = 'out' AND p.direction = 'orig')
                   )
                 ORDER BY t.packet_id, tn.id, t.timestamp ASC"
            )
            .bind(last_check_id)
            .fetch_all(&pool)
            .await {
                Ok(rows) => rows,
                Err(e) => {
                    debug!("Special trigger poll error: {}", e);
                    continue;
                }
            };

            if !triggers.is_empty() {
                last_check_id = triggers.iter().map(|t| t.0).max().unwrap_or(last_check_id);

                for (packet_id, start_idx, end_idx, _tag_id, tag_name, special_type, stream_id) in &triggers {
                    match special_type.as_str() {
                        "flag" => {
                            handle_flag(&pool, *packet_id, *start_idx, *end_idx, tag_name, *stream_id, &team_cache, &flag_queue).await;
                        }
                        "alert" => {
                            handle_alert(&broadcaster, *packet_id, *stream_id, tag_name);
                        }
                        _ => {}
                    }
                }
            }

            // Retry failed flag submissions
            {
                let farm_url = get_setting(&pool, "farm_url").await.unwrap_or_default();
                let farm_password = get_setting(&pool, "farm_password").await.unwrap_or_default();
                if !farm_url.is_empty() {
                    let mut q = flag_queue.write().await;
                    let mut i = 0;
                    while i < q.retry_queue.len() {
                        let entry = &q.retry_queue[i];
                        if entry.attempts >= MAX_RETRY_ATTEMPTS {
                            warn!("Flag '{}' exceeded max retries, dropping", entry.flag);
                            q.retry_queue.remove(i);
                            continue;
                        }
                        let payload = serde_json::json!([{
                            "flag": entry.flag,
                            "sploit": "red-eye",
                            "team": entry.team,
                        }]);
                        let client = reqwest::Client::new();
                        let mut req = client.post(&farm_url)
                            .json(&payload)
                            .timeout(std::time::Duration::from_secs(5));
                        if !farm_password.is_empty() {
                            req = req.header("Authorization", &farm_password);
                        }
                        match req.send().await {
                            Ok(resp) if resp.status().is_success() => {
                                info!("Retry succeeded for flag '{}': {}", entry.flag, resp.status());
                                let flag = q.retry_queue.remove(i).unwrap().flag;
                                q.mark_submitted(flag);
                            }
                            Ok(resp) => {
                                warn!("Retry rejected for flag '{}': {}", entry.flag, resp.status());
                                q.retry_queue[i].attempts += 1;
                                i += 1;
                            }
                            Err(_) => {
                                q.retry_queue[i].attempts += 1;
                                i += 1;
                            }
                        }
                    }
                }
            }
        }
    });
}

async fn resolve_team(
    ip: &str,
    farm_url: &str,
    farm_password: &str,
    cache: &RwLock<TeamCache>,
) -> String {
    // Try cache first
    {
        let c = cache.read().await;
        if let Some(name) = c.map.get(ip) {
            return name.clone();
        }
        if !c.is_stale() {
            return ip.to_string();
        }
    }

    // Cache miss or stale — refresh from farm
    if let Some(new_map) = fetch_team_map(farm_url, farm_password).await {
        let resolved = new_map.get(ip).cloned();
        let mut c = cache.write().await;
        c.map = new_map;
        c.last_refresh = std::time::Instant::now();
        return resolved.unwrap_or_else(|| ip.to_string());
    }

    ip.to_string()
}

async fn handle_flag(pool: &PgPool, packet_id: i32, start_idx: i32, end_idx: i32, tag_name: &str, stream_id: i32, team_cache: &Arc<RwLock<TeamCache>>, flag_queue: &Arc<RwLock<FlagQueue>>) {
    let farm_enabled = get_setting(pool, "farm_enabled").await.unwrap_or_default();
    if farm_enabled != "true" {
        debug!("Flag found but farm disabled (packet_id={})", packet_id);
        return;
    }

    let farm_url = match get_setting(pool, "farm_url").await {
        Some(url) if !url.is_empty() => url,
        _ => {
            warn!("Flag found but farm_url not configured");
            return;
        }
    };

    let farm_password = get_setting(pool, "farm_password").await.unwrap_or_default();

    // Extract matched text from packet
    let matched = match sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT substring(packet_data FROM $2 FOR $3) FROM packets WHERE id = $1"
    )
    .bind(packet_id)
    .bind(start_idx + 1) // SQL substring is 1-indexed
    .bind(end_idx - start_idx)
    .fetch_optional(pool)
    .await {
        Ok(Some(bytes)) => String::from_utf8_lossy(&bytes).to_string(),
        _ => {
            error!("Failed to extract flag text from packet {}", packet_id);
            return;
        }
    };

    // Check dedup — skip if already submitted or queued for retry
    {
        let q = flag_queue.read().await;
        if q.is_known(&matched) {
            debug!("Flag '{}' already submitted or queued, skipping", matched);
            return;
        }
    }

    // Resolve team: stream src_ip → team name via farm config
    let src_ip = match sqlx::query_scalar::<_, String>(
        "SELECT src_ip::text FROM stream WHERE id = $1"
    )
    .bind(stream_id)
    .fetch_optional(pool)
    .await {
        Ok(Some(ip)) => ip,
        _ => "unknown".to_string(),
    };

    let team = resolve_team(&src_ip, &farm_url, &farm_password, team_cache).await;

    info!("Submitting flag to farm: '{}' team={} (tag={}, packet={})", matched, team, tag_name, packet_id);

    // S4DFarm protocol: POST /api/post_flags with Authorization header
    let payload = serde_json::json!([{
        "flag": matched,
        "sploit": "red-eye",
        "team": team,
    }]);

    let client = reqwest::Client::new();
    let mut req = client.post(&farm_url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(5));

    if !farm_password.is_empty() {
        req = req.header("Authorization", &farm_password);
    }

    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            info!("Farm accepted flag '{}': {}", matched, resp.status());
            let mut q = flag_queue.write().await;
            q.mark_submitted(matched);
        }
        Ok(resp) => {
            warn!("Farm rejected flag '{}': {}", matched, resp.status());
            let mut q = flag_queue.write().await;
            q.retry_queue.push_back(FlagEntry {
                flag: matched,
                team,
                attempts: 1,
            });
        }
        Err(e) => {
            error!("Farm submit failed: {}", e);
            let mut q = flag_queue.write().await;
            q.retry_queue.push_back(FlagEntry {
                flag: matched,
                team,
                attempts: 1,
            });
        }
    }
}

fn handle_alert(
    broadcaster: &broadcast::Sender<String>,
    packet_id: i32,
    stream_id: i32,
    tag_name: &str,
) {
    let alert_event = serde_json::json!({
        "type": "alert",
        "tag_name": tag_name,
        "packet_id": packet_id,
        "stream_id": stream_id,
    });

    match broadcaster.send(alert_event.to_string()) {
        Ok(_) => debug!("Alert sent for tag '{}' packet {}", tag_name, packet_id),
        Err(_) => debug!("No WebSocket clients for alert"),
    }
}
