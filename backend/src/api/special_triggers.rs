use log::{debug, error, info, warn};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::api::settings::get_setting;

/// Background task: polls for new triggers with special_type (flag, alert).
/// Runs every 3 seconds.
pub fn spawn_special_trigger_handler(
    pool: PgPool,
    broadcaster: Arc<broadcast::Sender<String>>,
) {
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
                        handle_flag(&pool, *packet_id, *start_idx, *end_idx, tag_name).await;
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

async fn handle_flag(pool: &PgPool, packet_id: i32, start_idx: i32, end_idx: i32, tag_name: &str) {
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

    info!("Submitting flag to farm: '{}' (tag={}, packet={})", matched, tag_name, packet_id);

    // POST to farm
    let client = reqwest::Client::new();
    match client.post(&farm_url)
        .json(&serde_json::json!({"flags": [matched]}))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
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
