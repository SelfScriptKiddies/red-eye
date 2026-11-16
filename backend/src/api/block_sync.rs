use log::{error, info};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_service::AgentService;

/// Sync all block-family filters as drop filters to all agents.
/// Called when a filter with action=block/block_connection is created/updated/deleted.
pub async fn sync_block_filters(pool: &PgPool, agent_service: &Arc<AgentService>) {
    let block_tags = match sqlx::query_as::<_, (String, String, String, String, String)>(
        "SELECT template_type, template, direction, name, action
         FROM tag_name
         WHERE action IN ('block', 'block_connection')",
    )
    .fetch_all(pool)
    .await
    {
        Ok(tags) => tags,
        Err(e) => {
            error!("Failed to fetch block tags: {}", e);
            return;
        }
    };

    // Convert to agent FilterSync format
    let filters: Vec<serde_json::Value> = block_tags
        .iter()
        .map(|(ttype, template, direction, name, action)| {
            let dir = match direction.as_str() {
                "in" => "c2s",
                "out" => "s2c",
                _ => "both",
            };
            let kind = match ttype.as_str() {
                "regex" => "regex",
                "substr" => "substr",
                "bsubstr" => "bsubstr",
                _ => "substr",
            };
            let filter_action = if action == "block_connection" {
                "drop_connection"
            } else {
                "drop"
            };
            serde_json::json!({
                "name": name,
                "kind": kind,
                "pattern": template,
                "direction": dir,
                "action": filter_action
            })
        })
        .collect();

    let command = serde_json::json!({
        "type": "FilterSync",
        "filters": filters
    });

    let results = agent_service.broadcast_command(&[], &command).await;
    let success = results.iter().filter(|(_, r)| r.is_ok()).count();
    let total = results.len();

    info!(
        "Block sync: {} filters sent to {}/{} agents",
        filters.len(),
        success,
        total
    );
}
