use sqlx::Error;

use super::user_agent_dto::{
    UA_LIMIT_MAX, UaCursorKey, UaOrder, UaSort, UserAgentsResponse, decode_cursor, encode_cursor,
};
use super::user_agent_repository::UserAgentRepository;

/// Client-error reasons separated from infrastructure errors so the
/// handler can answer 400/404 without an unbounded fallback query.
#[derive(Debug)]
pub enum UaError {
    BadRequest(String),
    ServiceNotFound(i64),
    Database(Error),
}

impl From<Error> for UaError {
    fn from(e: Error) -> Self {
        UaError::Database(e)
    }
}

pub struct UserAgentService {
    repository: UserAgentRepository,
}

impl UserAgentService {
    pub fn new_from_pgpool(pgpool: sqlx::PgPool) -> Self {
        Self {
            repository: UserAgentRepository::new(pgpool),
        }
    }

    /// Parse and validate the query parameters. Anything malformed is a
    /// client error; there is no silent fallback to an unbounded query.
    pub async fn list(
        &self,
        service_id: i64,
        agent_id: Option<&str>,
        limit: i64,
        cursor: Option<&str>,
        sort: Option<&str>,
        order: Option<&str>,
    ) -> Result<UserAgentsResponse, UaError> {
        let sort = UaSort::parse(sort).map_err(UaError::BadRequest)?;
        let order = UaOrder::parse(order).map_err(UaError::BadRequest)?;
        if limit < 1 || limit > UA_LIMIT_MAX {
            return Err(UaError::BadRequest(format!(
                "limit must be between 1 and {}",
                UA_LIMIT_MAX
            )));
        }
        let agent_id = agent_id.filter(|a| !a.is_empty());
        if !self.repository.service_exists(service_id).await? {
            return Err(UaError::ServiceNotFound(service_id));
        }
        let cursor_key = match cursor {
            Some(raw) => Some(
                decode_cursor(raw, service_id, agent_id, sort, order)
                    .map_err(UaError::BadRequest)?,
            ),
            None => None,
        };

        // Fetch one extra row to detect the next page; it is dropped.
        let mut items = self
            .repository
            .list_user_agents(
                service_id,
                agent_id,
                sort,
                order,
                limit + 1,
                cursor_key.as_ref(),
            )
            .await?;
        let mut next_cursor = None;
        if items.len() > limit as usize {
            items.truncate(limit as usize);
            if let Some(last) = items.last() {
                next_cursor = Some(encode_cursor(
                    service_id,
                    agent_id,
                    sort,
                    order,
                    &UaCursorKey {
                        message_count: last.message_count,
                        last_seen: last.last_seen,
                        user_agent: last.user_agent.clone().unwrap_or_default(),
                    },
                ));
            }
        }

        let overflow = self
            .repository
            .overflow_summary(service_id, agent_id)
            .await?;
        let has_overflow = overflow.is_some();
        // The overflow summary is a fixed tail item: it appears once the
        // ordinary pages run out, never as a clickable exact value.
        if next_cursor.is_none() {
            if let Some(overflow_item) = overflow {
                items.push(overflow_item);
            }
        }

        let aggregation_started = self.repository.aggregation_started().await?;
        Ok(UserAgentsResponse {
            items,
            next_cursor,
            has_overflow,
            aggregation_started,
        })
    }

    /// Bounded online cleanup of buckets fully older than the retention
    /// cutoff: up to `max_batches` batches per call, leftovers reported as
    /// pending for the next cleanup tick.
    pub async fn cleanup_expired_buckets(
        &self,
        cutoff: i64,
        batch_size: i64,
        max_batches: usize,
    ) -> Result<(u64, u64), Error> {
        let mut deleted_total: u64 = 0;
        for _ in 0..max_batches {
            let deleted = self
                .repository
                .delete_expired_buckets(cutoff, batch_size)
                .await?;
            deleted_total += deleted;
            if deleted == 0 {
                return Ok((deleted_total, 0));
            }
        }
        let pending = self.repository.count_expired_buckets(cutoff).await?;
        Ok((deleted_total, pending.max(0) as u64))
    }

    /// Remove all aggregate rows of a deleted service in bounded batches.
    pub async fn cleanup_service_buckets(
        &self,
        service_id: i64,
        batch_size: i64,
        max_batches: usize,
    ) -> Result<(u64, u64), Error> {
        let mut deleted_total: u64 = 0;
        for _ in 0..max_batches {
            let deleted = self
                .repository
                .delete_service_buckets(service_id, batch_size)
                .await?;
            deleted_total += deleted;
            if deleted == 0 {
                return Ok((deleted_total, 0));
            }
        }
        let pending = self.repository.count_service_buckets(service_id).await?;
        Ok((deleted_total, pending.max(0) as u64))
    }
}
