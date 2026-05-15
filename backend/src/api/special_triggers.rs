use log::{debug, error, info, warn};
use sqlx::PgPool;
use std::collections::HashMap;
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

    tokio::spawn(async move {
        use tokio::time::{interval, Duration};
        let mut ticker = interval(Duration::from_secs(3));
        let mut last_check_ts: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        loop {
            ticker.tick().await;

            let now_ts: i64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            // Find triggers with special_type since last check (deduplicated per packet+tag)
            let triggers = match sqlx::query_as::<_, (i32, i32, i32, i32, String, String, i32)>(
                "SELECT DISTINCT ON (t.packet_id, tn.id)
                        t.packet_id, t.start_index, t.end_index, tn.id as tag_id,
                        tn.name as tag_name, tn.special_type, p.stream_id
                 FROM tag t
                 JOIN tag_name tn ON tn.id = t.tag_id
                 JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
                 WHERE tn.special_type IS NOT NULL
                   AND t.timestamp >= $1
                 ORDER BY t.packet_id, tn.id, t.timestamp ASC"
            )
            .bind(last_check_ts)
            .fetch_all(&pool)
            .await {
                Ok(rows) => rows,
                Err(e) => {
                    debug!("Special trigger poll error: {}", e);
                    continue;
                }
            };

            last_check_ts = now_ts;

            if triggers.is_empty() {
                continue;
            }

            for (packet_id, start_idx, end_idx, _tag_id, tag_name, special_type, stream_id) in &triggers {
                match special_type.as_str() {
                    "flag" => {
                        handle_flag(&pool, *packet_id, *start_idx, *end_idx, tag_name, *stream_id, &team_cache).await;
                    }
                    "alert" => {
                        handle_alert(&broadcaster, *packet_id, *stream_id, tag_name);
                    }
                    _ => {}
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

async fn handle_flag(pool: &PgPool, packet_id: i32, start_idx: i32, end_idx: i32, tag_name: &str, stream_id: i32, team_cache: &Arc<RwLock<TeamCache>>) {
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
        Ok(resp) => {
            info!("Farm response: {} for flag '{}'", resp.status(), matched);
        }
        Err(e) => {
            error!("Farm submit failed: {}", e);
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
