use sqlx::{PgPool, Postgres, Transaction};

/// Row shape of proxy_protection joined with the service identity.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ProtectionRow {
    pub service_id: i64,
    pub service_name: String,
    pub service_port: i32,
    pub protocol: String,
    pub enabled: bool,
    pub listen_address: Option<String>,
    pub listen_port: Option<i32>,
    pub target_address: Option<String>,
    pub target_port: Option<i32>,
    pub tls_mode: Option<String>,
    pub http2_enabled: bool,
    pub revision: i64,
    pub effective: bool,
    pub effective_revision: Option<i64>,
    pub reported_state: Option<String>,
    pub last_error: Option<String>,
}

pub struct ProtectRepository {
    pool: PgPool,
}

impl ProtectRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn get_row(
        &self,
        agent_id: &str,
        service_id: i64,
    ) -> Result<Option<ProtectionRow>, sqlx::Error> {
        sqlx::query_as::<_, ProtectionRow>(
            r#"
            SELECT p.service_id, s.name AS service_name, s.port AS service_port,
                   s.protocol AS protocol, p.enabled, p.listen_address, p.listen_port,
                   p.target_address, p.target_port, p.tls_mode, p.http2_enabled,
                   p.revision, p.effective, p.effective_revision, p.reported_state,
                   p.last_error
            FROM proxy_protection p
            JOIN service s ON s.id = p.service_id
            WHERE p.agent_id = $1 AND p.service_id = $2
            "#,
        )
        .bind(agent_id)
        .bind(service_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// All protection rows for one agent (desired + diagnostics).
    pub async fn rows_for_agent(&self, agent_id: &str) -> Result<Vec<ProtectionRow>, sqlx::Error> {
        sqlx::query_as::<_, ProtectionRow>(
            r#"
            SELECT p.service_id, s.name AS service_name, s.port AS service_port,
                   s.protocol AS protocol, p.enabled, p.listen_address, p.listen_port,
                   p.target_address, p.target_port, p.tls_mode, p.http2_enabled,
                   p.revision, p.effective, p.effective_revision, p.reported_state,
                   p.last_error
            FROM proxy_protection p
            JOIN service s ON s.id = p.service_id
            WHERE p.agent_id = $1
            ORDER BY p.service_id
            "#,
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await
    }

    /// Listen ports of this agent's enabled proxy rules: the port
    /// allocator must consider persisted rules, not just the last
    /// heartbeat (backend restarts must not re-allocate a bound port).
    pub async fn enabled_listen_ports(&self, agent_id: &str) -> Result<Vec<i32>, sqlx::Error> {
        sqlx::query_scalar::<_, i32>(
            "SELECT listen_port FROM proxy_protection
             WHERE agent_id = $1 AND enabled AND listen_port IS NOT NULL",
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await
    }

    /// Persist the desired state for one enable inside the caller's
    /// transaction (the revision bump happens in the same transaction).
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_enabled(
        tx: &mut Transaction<'_, Postgres>,
        agent_id: &str,
        service_id: i64,
        listen_address: &str,
        listen_port: i32,
        target_address: &str,
        target_port: i32,
        protocol: &str,
        tls_mode: Option<&str>,
        http2_enabled: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO proxy_protection
                (agent_id, service_id, enabled, listen_address, listen_port,
                 target_address, target_port, protocol, tls_mode, http2_enabled,
                 revision, updated_at)
            VALUES ($1, $2, TRUE, $3, $4, $5, $6, $7, $8, $9, 0, now())
            ON CONFLICT (agent_id, service_id) DO UPDATE SET
                enabled = TRUE,
                listen_address = EXCLUDED.listen_address,
                listen_port = EXCLUDED.listen_port,
                target_address = EXCLUDED.target_address,
                target_port = EXCLUDED.target_port,
                protocol = EXCLUDED.protocol,
                tls_mode = EXCLUDED.tls_mode,
                http2_enabled = EXCLUDED.http2_enabled,
                updated_at = now()
            "#,
        )
        .bind(agent_id)
        .bind(service_id)
        .bind(listen_address)
        .bind(listen_port)
        .bind(target_address)
        .bind(target_port)
        .bind(protocol)
        .bind(tls_mode)
        .bind(http2_enabled)
        .execute(tx.as_mut())
        .await?;
        Ok(())
    }

    pub async fn set_enabled(
        tx: &mut Transaction<'_, Postgres>,
        agent_id: &str,
        service_id: i64,
        enabled: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE proxy_protection
            SET enabled = $3, updated_at = now()
            WHERE agent_id = $1 AND service_id = $2
            "#,
        )
        .bind(agent_id)
        .bind(service_id)
        .bind(enabled)
        .execute(tx.as_mut())
        .await?;
        Ok(())
    }

    pub async fn delete_row(&self, agent_id: &str, service_id: i64) -> Result<u64, sqlx::Error> {
        let result =
            sqlx::query("DELETE FROM proxy_protection WHERE agent_id = $1 AND service_id = $2")
                .bind(agent_id)
                .bind(service_id)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected())
    }

    /// Store agent-reported diagnostics. Never touches desired columns:
    /// the agent cannot overwrite operator state.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_effective_diag(
        &self,
        agent_id: &str,
        service_id: i64,
        effective: bool,
        effective_revision: i64,
        reported_state: &str,
        last_error: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE proxy_protection
            SET effective = $3, effective_revision = $4, reported_state = $5,
                last_error = $6, updated_at = now()
            WHERE agent_id = $1 AND service_id = $2
            "#,
        )
        .bind(agent_id)
        .bind(service_id)
        .bind(effective)
        .bind(effective_revision)
        .bind(reported_state)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Desired proxies of one agent for the configuration snapshot.
    pub async fn enabled_desired(&self, agent_id: &str) -> Result<Vec<ProtectionRow>, sqlx::Error> {
        sqlx::query_as::<_, ProtectionRow>(
            r#"
            SELECT p.service_id, s.name AS service_name, s.port AS service_port,
                   s.protocol AS protocol, p.enabled, p.listen_address, p.listen_port,
                   p.target_address, p.target_port, p.tls_mode, p.http2_enabled,
                   p.revision, p.effective, p.effective_revision, p.reported_state,
                   p.last_error
            FROM proxy_protection p
            JOIN service s ON s.id = p.service_id
            WHERE p.agent_id = $1 AND p.enabled
            ORDER BY p.service_id
            "#,
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await
    }
}
