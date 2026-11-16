use std::collections::HashMap;

use sqlx::Error;

use crate::api::agent::agent_config::{ControlPlane, SnapshotReason};
use crate::api::service::service_dto::{MutationResult, Service};
use crate::api::service::service_repository::ServiceRepository;

/// Detect the (name, port, protocol) uniqueness violation so handlers can
/// answer 409 instead of a generic 500.
pub fn is_unique_service_violation(error: &Error) -> bool {
    if let Error::Database(db_err) = error {
        return db_err.constraint() == Some("unique_service_name_port_protocol");
    }
    false
}

pub struct ServiceService {
    repository: ServiceRepository,
    control: std::sync::Arc<ControlPlane>,
}

impl ServiceService {
    pub fn new_from_pgpool(pgpool: sqlx::PgPool, control: std::sync::Arc<ControlPlane>) -> Self {
        ServiceService {
            repository: ServiceRepository::new(pgpool),
            control,
        }
    }

    pub async fn get_service_by_id(&self, id: i64) -> Result<Service, Error> {
        let mut conn = self.repository.pool().acquire().await?;
        let mut service = ServiceRepository::get_service_by_id(&mut conn, id).await?;
        if !service.is_global {
            service.agent_ids = self
                .repository
                .get_agent_ids_by_service(id)
                .await
                .unwrap_or_default();
        }
        Ok(service)
    }

    pub async fn get_all_services(&self) -> Result<Vec<Service>, Error> {
        let mut services = self.repository.get_all_services().await?;
        let bindings = self.repository.get_all_agent_bindings().await?;
        let mut by_service: HashMap<i64, Vec<String>> = HashMap::new();
        for (service_id, agent_id) in bindings {
            by_service.entry(service_id).or_default().push(agent_id);
        }
        for service in &mut services {
            if !service.is_global {
                service.agent_ids = by_service.remove(&service.id).unwrap_or_default();
            }
        }
        Ok(services)
    }

    /// Create in one transaction with the revision bump; publish after
    /// commit. Publication failure leaves the change as pending sync
    /// (sync = "pending"), never rolls back the committed row.
    pub async fn create_service(
        &self,
        service: &Service,
        agent_ids: &[String],
    ) -> Result<MutationResult, Error> {
        let mut tx = self.repository.pool().begin().await?;
        let created = ServiceRepository::insert_service(tx.as_mut(), service).await?;
        if !created.is_global {
            ServiceRepository::replace_assignments(tx.as_mut(), created.id, agent_ids).await?;
        }
        let revision = ControlPlane::bump_revision(&mut tx).await?;
        tx.commit().await?;

        let sync = self.publish_service_op(&created, UpdateKind::Add).await;
        Ok(MutationResult {
            service: created,
            revision,
            sync,
        })
    }

    /// Update in one transaction with the revision bump. When the payload
    /// carries an explicit agent list it becomes the complete assignment
    /// set; switching to global clears agent_service rows.
    pub async fn update_service(
        &self,
        service: &Service,
        agent_ids: Option<Vec<String>>,
    ) -> Result<MutationResult, Error> {
        let mut tx = self.repository.pool().begin().await?;
        let updated = ServiceRepository::update_service_row(tx.as_mut(), service).await?;
        if updated.is_global {
            ServiceRepository::clear_assignments(tx.as_mut(), updated.id).await?;
        } else if let Some(ids) = &agent_ids {
            ServiceRepository::replace_assignments(tx.as_mut(), updated.id, ids).await?;
        }
        let revision = ControlPlane::bump_revision(&mut tx).await?;
        tx.commit().await?;

        let sync = self.publish_service_op(&updated, UpdateKind::Update).await;
        Ok(MutationResult {
            service: updated,
            revision,
            sync,
        })
    }

    pub async fn delete_service(&self, id: i64) -> Result<MutationResult, Error> {
        let mut conn = self.repository.pool().acquire().await?;
        let existing = ServiceRepository::get_service_by_id(&mut conn, id).await?;
        let mut tx = self.repository.pool().begin().await?;
        ServiceRepository::delete_service_row(tx.as_mut(), id).await?;
        let revision = ControlPlane::bump_revision(&mut tx).await?;
        // The FK cascade already removed the service's explicit filter
        // targets. An explicitly targeted filter left with no services is
        // disabled and flagged as requiring targets (shown in the UI);
        // global filters are unaffected.
        sqlx::query(
            "UPDATE tag_name SET enabled = FALSE, revision = $1 \
             WHERE NOT service_targets_global \
               AND NOT EXISTS (SELECT 1 FROM filter_service_target WHERE filter_id = tag_name.id)",
        )
        .bind(revision)
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        let sync = self.publish_service_op(&existing, UpdateKind::Remove).await;
        Ok(MutationResult {
            service: existing,
            revision,
            sync,
        })
    }

    /// Edit one agent's explicit assignment for a non-global service inside
    /// a transaction; returns the new revision.
    pub async fn bind_agent_service(
        &self,
        agent_id: &str,
        service_id: i64,
    ) -> Result<(bool, i64), Error> {
        let mut conn = self.repository.pool().acquire().await?;
        let service = ServiceRepository::get_service_by_id(&mut conn, service_id).await?;
        if service.is_global {
            return Ok((false, self.control.current_revision().await.unwrap_or(0)));
        }
        let mut tx = self.repository.pool().begin().await?;
        ServiceRepository::bind_agent(tx.as_mut(), agent_id, service_id).await?;
        let revision = ControlPlane::bump_revision(&mut tx).await?;
        tx.commit().await?;
        Ok((true, revision))
    }

    pub async fn unbind_agent_service(
        &self,
        agent_id: &str,
        service_id: i64,
    ) -> Result<(bool, i64, u64), Error> {
        let mut conn = self.repository.pool().acquire().await?;
        let service = ServiceRepository::get_service_by_id(&mut conn, service_id).await?;
        if service.is_global {
            return Ok((false, self.control.current_revision().await.unwrap_or(0), 0));
        }
        let mut tx = self.repository.pool().begin().await?;
        let removed = ServiceRepository::unbind_agent(tx.as_mut(), agent_id, service_id).await?;
        let revision = ControlPlane::bump_revision(&mut tx).await?;
        tx.commit().await?;
        Ok((true, revision, removed))
    }

    /// Post-commit publication to analyzers (the incremental op for prompt
    /// delivery). Agents learn the change via revisioned snapshots, so no
    /// per-agent fan-out here. Returns "synced" or "pending".
    async fn publish_service_op(&self, service: &Service, kind: UpdateKind) -> &'static str {
        let updater = self.control.updater();
        let id = u64::try_from(service.id).unwrap_or_default();
        let result = match kind {
            UpdateKind::Add => updater.add_service(service.clone().into(), id).await,
            UpdateKind::Update => updater.update_service(id, service.clone().into()).await,
            UpdateKind::Remove => updater.remove_service(id).await,
        };

        match result {
            Ok(()) => "synced",
            Err(e) => {
                // The committed change stays visible as pending sync; the
                // 5-minute ConfigSync and agent heartbeats converge it.
                log::error!(
                    "Service '{}' (id {}) committed but publication failed (pending sync): {}",
                    service.name,
                    service.id,
                    e
                );
                "pending"
            }
        }
    }

    /// Ask the control plane to publish a fresh snapshot to one agent
    /// (used after explicit assignment edits).
    pub async fn notify_agent(&self, agent_id: &str) {
        let _ = self
            .control
            .send_snapshot(agent_id, SnapshotReason::StaleRevision)
            .await;
    }
}

enum UpdateKind {
    Add,
    Update,
    Remove,
}
