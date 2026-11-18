use crate::api::tag::tag_dto::{Tag, TagName, TagNameCreate};
use ipnetwork::IpNetwork;
use sqlx::PgPool;

/// Row shape of tag_name with the unified model columns.
#[derive(sqlx::FromRow)]
struct TagNameRow {
    id: i64,
    name: String,
    color: i16,
    template_type: String,
    template: String,
    direction: String,
    special_type: Option<String>,
    action: String,
    enabled: bool,
    ip_network: Option<IpNetwork>,
    ip_endpoint: Option<String>,
    expression_source: Option<String>,
    service_targets_global: bool,
    agent_targets_global: bool,
    threshold_count: Option<i32>,
    threshold_window_secs: Option<i32>,
    threshold_group_by: Option<String>,
    threshold_cooldown_secs: Option<i32>,
    threshold_block_secs: Option<i32>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revision: i64,
}

impl TagNameRow {
    fn into_dto(self, service_ids: Vec<i64>, agent_ids: Vec<String>) -> TagName {
        TagName {
            id: self.id,
            name: self.name,
            color: self.color,
            template_type: self.template_type,
            template: self.template,
            direction: self.direction,
            special_type: self.special_type,
            action: self.action,
            enabled: self.enabled,
            ip_network: self.ip_network.map(|n| n.to_string()),
            ip_endpoint: self.ip_endpoint,
            expression_source: self.expression_source,
            service_targets_global: self.service_targets_global,
            agent_targets_global: self.agent_targets_global,
            service_ids,
            agent_ids,
            threshold_count: self.threshold_count,
            threshold_window_secs: self.threshold_window_secs,
            threshold_group_by: self.threshold_group_by,
            threshold_cooldown_secs: self.threshold_cooldown_secs,
            threshold_block_secs: self.threshold_block_secs,
            expires_at: self.expires_at,
            revision: self.revision,
        }
    }
}

pub struct TagRepository {
    pool: PgPool,
}

const ROW_COLUMNS: &str = "id, name, color, template_type, template, direction, special_type, action, enabled, \
     ip_network, ip_endpoint, expression_source, service_targets_global, agent_targets_global, \
     threshold_count, threshold_window_secs, threshold_group_by, threshold_cooldown_secs, \
     threshold_block_secs, expires_at, revision";

impl TagRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn targets_for(
        pool: &PgPool,
        ids: &[i64],
    ) -> Result<(Vec<(i64, i64)>, Vec<(i64, String)>), sqlx::Error> {
        if ids.is_empty() {
            return Ok((vec![], vec![]));
        }
        let services = sqlx::query_as::<_, (i64, i64)>(
            "SELECT filter_id, service_id FROM filter_service_target WHERE filter_id = ANY($1)",
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;
        let agents = sqlx::query_as::<_, (i64, String)>(
            "SELECT filter_id, agent_id FROM filter_agent_target WHERE filter_id = ANY($1)",
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;
        Ok((services, agents))
    }

    fn assemble(
        rows: Vec<TagNameRow>,
        service_pairs: Vec<(i64, i64)>,
        agent_pairs: Vec<(i64, String)>,
    ) -> Vec<TagName> {
        rows.into_iter()
            .map(|row| {
                let id = row.id;
                let service_ids = service_pairs
                    .iter()
                    .filter(|(f, _)| *f == id)
                    .map(|(_, s)| *s)
                    .collect();
                let agent_ids = agent_pairs
                    .iter()
                    .filter(|(f, _)| *f == id)
                    .map(|(_, a)| a.clone())
                    .collect();
                row.into_dto(service_ids, agent_ids)
            })
            .collect()
    }

    /// Insert the tag_name row and its explicit targets in one transaction
    /// with the control-plane revision bump, returning the stored filter.
    /// Commit happens here; publication is the caller's job.
    pub async fn create_tag_name(
        &self,
        tag_name: TagNameCreate,
        action: String,
        ip_network: Option<IpNetwork>,
    ) -> Result<TagName, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let revision =
            crate::api::agent::agent_config::ControlPlane::bump_revision(&mut tx).await?;
        let row = sqlx::query_as::<_, TagNameRow>(
            "INSERT INTO tag_name (name, color, template_type, template, direction, action, \
             enabled, ip_network, ip_endpoint, expression_source, service_targets_global, \
             agent_targets_global, threshold_count, threshold_window_secs, threshold_group_by, \
             threshold_cooldown_secs, threshold_block_secs, expires_at, revision)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
             RETURNING *",
        )
        .bind(&tag_name.name)
        .bind(tag_name.color)
        .bind(&tag_name.template_type)
        .bind(&tag_name.template)
        .bind(&tag_name.direction)
        .bind(&action)
        .bind(tag_name.enabled)
        .bind(ip_network)
        .bind(tag_name.ip_endpoint.clone())
        .bind(tag_name.expression_source.clone())
        .bind(tag_name.service_targets_global)
        .bind(tag_name.agent_targets_global)
        .bind(tag_name.threshold_count)
        .bind(tag_name.threshold_window_secs)
        .bind(tag_name.threshold_group_by.clone())
        .bind(tag_name.threshold_cooldown_secs)
        .bind(tag_name.threshold_block_secs)
        .bind(tag_name.expires_at)
        .bind(revision)
        .fetch_one(tx.as_mut())
        .await?;
        let id = row.id;
        insert_targets(tx.as_mut(), id, &tag_name).await?;
        tx.commit().await?;

        let (services, agents) = Self::targets_for(&self.pool, &[id]).await?;
        Ok(Self::assemble(vec![row], services, agents).remove(0))
    }

    pub async fn get_tags_by_packet_id(&self, packet_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        sqlx::query_as::<_, Tag>(
            "SELECT id, packet_id, tag_id, start_index, end_index
             FROM tag
             WHERE packet_id = $1",
        )
        .bind(packet_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_tag_name_by_id(&self, id: i64) -> Result<TagName, sqlx::Error> {
        let row = sqlx::query_as::<_, TagNameRow>(&format!(
            "SELECT {} FROM tag_name WHERE id = $1",
            ROW_COLUMNS
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        let (services, agents) = Self::targets_for(&self.pool, &[id]).await?;
        Ok(Self::assemble(vec![row], services, agents).remove(0))
    }

    pub async fn update_tag_name(
        &self,
        id: i64,
        tag_name: TagNameCreate,
        action: String,
        ip_network: Option<IpNetwork>,
    ) -> Result<TagName, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let revision =
            crate::api::agent::agent_config::ControlPlane::bump_revision(&mut tx).await?;
        let row = sqlx::query_as::<_, TagNameRow>(
            "UPDATE tag_name SET name = $1, color = $2, template_type = $3, template = $4, \
             direction = $5, action = $6, enabled = $7, ip_network = $8, ip_endpoint = $9, \
             expression_source = $10, service_targets_global = $11, agent_targets_global = $12, \
             threshold_count = $13, threshold_window_secs = $14, threshold_group_by = $15, \
             threshold_cooldown_secs = $16, threshold_block_secs = $17, expires_at = $18, \
             revision = $19
             WHERE id = $20
             RETURNING *",
        )
        .bind(&tag_name.name)
        .bind(tag_name.color)
        .bind(&tag_name.template_type)
        .bind(&tag_name.template)
        .bind(&tag_name.direction)
        .bind(&action)
        .bind(tag_name.enabled)
        .bind(ip_network)
        .bind(tag_name.ip_endpoint.clone())
        .bind(tag_name.expression_source.clone())
        .bind(tag_name.service_targets_global)
        .bind(tag_name.agent_targets_global)
        .bind(tag_name.threshold_count)
        .bind(tag_name.threshold_window_secs)
        .bind(tag_name.threshold_group_by.clone())
        .bind(tag_name.threshold_cooldown_secs)
        .bind(tag_name.threshold_block_secs)
        .bind(tag_name.expires_at)
        .bind(revision)
        .bind(id)
        .fetch_one(tx.as_mut())
        .await?;
        // Explicit targets are a complete replacement set: rewrite them.
        sqlx::query("DELETE FROM filter_service_target WHERE filter_id = $1")
            .bind(id)
            .execute(tx.as_mut())
            .await?;
        sqlx::query("DELETE FROM filter_agent_target WHERE filter_id = $1")
            .bind(id)
            .execute(tx.as_mut())
            .await?;
        insert_targets(tx.as_mut(), id, &tag_name).await?;
        tx.commit().await?;

        let (services, agents) = Self::targets_for(&self.pool, &[id]).await?;
        Ok(Self::assemble(vec![row], services, agents).remove(0))
    }

    pub async fn delete_tag(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM tag WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_tag_name(&self, id: i64) -> Result<i64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // Deleting a filter must also drop its unexpired active blocks (FK
        // cascade) and advance the revision so agents unban promptly via
        // the next snapshot.
        let revision =
            crate::api::agent::agent_config::ControlPlane::bump_revision(&mut tx).await?;
        sqlx::query("DELETE FROM tag_name WHERE id = $1")
            .bind(id)
            .execute(tx.as_mut())
            .await?;
        tx.commit().await?;
        Ok(revision)
    }

    /// Enable/disable toggle without rewriting the rest of the definition;
    /// bumps the revision so consumers converge.
    pub async fn set_tag_name_enabled(&self, id: i64, enabled: bool) -> Result<i64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let revision =
            crate::api::agent::agent_config::ControlPlane::bump_revision(&mut tx).await?;
        sqlx::query("UPDATE tag_name SET enabled = $2, revision = $3 WHERE id = $1")
            .bind(id)
            .bind(enabled)
            .bind(revision)
            .execute(tx.as_mut())
            .await?;
        tx.commit().await?;
        Ok(revision)
    }

    pub async fn get_tags_by_stream_id(&self, stream_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        // Live + saved union deduplicated by original tag id (saved copies
        // exist while a starred stream is being archived or after its live
        // partition dropped).
        sqlx::query_as::<_, Tag>(
            r#"
            SELECT id, name, color, packet_id, tag_id, start_index, end_index
            FROM (
                SELECT DISTINCT ON (id) id, name, color, packet_id, tag_id, start_index, end_index
                FROM (
                    SELECT t.id, tn.name, tn.color, t.packet_id, t.tag_id, t.start_index, t.end_index, 0 AS src
                    FROM tag t
                    JOIN packets p ON t.packet_id = p.id
                    JOIN tag_name tn ON t.tag_id = tn.id
                    WHERE p.stream_id = $1
                    UNION ALL
                    SELECT st.id, tn.name, tn.color, st.packet_id, st.tag_id, st.start_index, st.end_index, 1 AS src
                    FROM saved_tag st
                    JOIN tag_name tn ON st.tag_id = tn.id
                    WHERE st.stream_id = $1
                ) u
                ORDER BY id, src
            ) d
            ORDER BY packet_id, id
            "#,
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_all_tag_names(&self) -> Result<Vec<TagName>, sqlx::Error> {
        let rows = sqlx::query_as::<_, TagNameRow>(&format!(
            "SELECT {} FROM tag_name ORDER BY name",
            ROW_COLUMNS
        ))
        .fetch_all(&self.pool)
        .await?;
        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        let (services, agents) = Self::targets_for(&self.pool, &ids).await?;
        Ok(Self::assemble(rows, services, agents))
    }

    /// Get flag incoming/outgoing counts per service for the last N minutes
    pub async fn get_flag_balance(
        &self,
        minutes: i32,
    ) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, String, i64, i64)>(
            "SELECT s.id as service_id, s.name as service_name,
                    COUNT(*) FILTER (WHERE p.direction = 'dest') as incoming,
                    COUNT(*) FILTER (WHERE p.direction = 'orig') as outgoing
             FROM tag t
             JOIN tag_name tn ON tn.id = t.tag_id
             JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
             JOIN stream str ON str.id = p.stream_id
             JOIN service s ON s.id = str.service_id
             WHERE tn.action = 'flag'
               AND t.timestamp >= (EXTRACT(EPOCH FROM NOW())::BIGINT - ($1 * 60))
             GROUP BY s.id, s.name
             ORDER BY s.name ASC",
        )
        .bind(minutes)
        .fetch_all(&self.pool)
        .await
    }

    /// Get tag trigger counts aggregated by minute for the last N minutes
    pub async fn get_tag_metrics(
        &self,
        minutes: i32,
    ) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, String, i64, i64)>(
            "SELECT tn.id, tn.name,
                    (t.timestamp / 60) * 60 as minute_ts,
                    COUNT(*) as trigger_count
             FROM tag t
             JOIN tag_name tn ON tn.id = t.tag_id
             WHERE t.timestamp >= (EXTRACT(EPOCH FROM NOW())::BIGINT - ($1 * 60))
             GROUP BY tn.id, tn.name, minute_ts
             ORDER BY minute_ts ASC",
        )
        .bind(minutes)
        .fetch_all(&self.pool)
        .await
    }
}

async fn insert_targets(
    tx: &mut sqlx::PgConnection,
    filter_id: i64,
    tag_name: &TagNameCreate,
) -> Result<(), sqlx::Error> {
    // Target sets are small (tens at most): plain per-row inserts beat
    // unnest plumbing here.
    if !tag_name.service_targets_global {
        for service_id in &tag_name.service_ids {
            sqlx::query(
                "INSERT INTO filter_service_target (filter_id, service_id) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
            )
            .bind(filter_id)
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    if !tag_name.agent_targets_global {
        for agent_id in &tag_name.agent_ids {
            sqlx::query(
                "INSERT INTO filter_agent_target (filter_id, agent_id) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
            )
            .bind(filter_id)
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    Ok(())
}
