use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;

use super::favorite_dto::{FavoriteStateResponse, FavoriteUsageResponse};
use crate::api::metrics;
use crate::configuration::ApplicationConfig;
use log::{error, info, warn};

/// Fixed advisory-lock pair serializing star/unstar admissions so two
/// concurrent stars cannot both pass the same capacity snapshot.
const FAVORITE_LOCK_KEY: (i64, i64) = (1_908_874_337, 7);

/// Settings keys (see database/init/07_settings.sql). Env values seed these
/// rows at startup; workers re-read them so operator changes apply without
/// restarting ingestion.
pub const SETTING_MAX_TOTAL: &str = "favorite_max_total_bytes";
pub const SETTING_SEGMENT: &str = "favorite_segment_bytes";
pub const SETTING_DISK_RESERVE: &str = "favorite_disk_reserve_bytes";

const ARCHIVE_BATCH_ROWS: i64 = 1000;
const ARCHIVE_CANDIDATES_PER_TICK: i64 = 20;
const ARCHIVE_TICK_MS: u64 = 2000;
const ARCHIVE_BACKOFF_MAX_MS: u64 = 60_000;
const CLEANUP_BATCH_ROWS: i64 = 1000;
const CLEANUP_BATCHES_PER_STREAM_PER_TICK: usize = 4;
const CLEANUP_TICK_MS: u64 = 5000;
const CLEANUP_CANDIDATES_PER_TICK: i64 = 10;

pub(crate) struct FavoriteService {
    pgpool: PgPool,
}

#[derive(Debug, sqlx::FromRow)]
struct FavoriteRow {
    favorite: bool,
    active: bool,
    retention_state: String,
    favorite_revision: i64,
    retention_bytes: i64,
    archived_through_packet_id: i64,
    segment_index: i32,
    #[sqlx(default)]
    archive_error: Option<String>,
}

impl FavoriteRow {
    fn to_response(&self, id: i64) -> FavoriteStateResponse {
        FavoriteStateResponse {
            id,
            favorite: self.favorite,
            favorite_revision: self.favorite_revision,
            retention_state: self.retention_state.clone(),
            segment_index: self.segment_index,
            retention_bytes: self.retention_bytes,
            archived_through_packet_id: self.archived_through_packet_id,
            archive_error: self.archive_error.clone(),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct SavedHttpRow {
    id: i64,
    stream_id: i64,
    packet_id: i64,
    is_request: bool,
    method: Option<String>,
    path: Option<String>,
    status_code: Option<i16>,
    http_version: Option<String>,
    content_type: Option<String>,
    content_length: Option<i32>,
    body_offset: Option<i32>,
    body_length: Option<i32>,
    header_set_id: Option<i64>,
    timestamp: i64,
}

pub(crate) enum StarOutcome {
    Unchanged(FavoriteStateResponse),
    Started(FavoriteStateResponse),
    CapacityExceeded {
        total_reserved: i64,
        reservation: i64,
        max_total: i64,
    },
}

// Capacity accounting helpers (pure, unit-tested at the bottom of this
// file).

/// Reservation one stream adds when starred: an active segment may still
/// grow up to the full segment limit; a closed one is bounded by its
/// measured size.
pub fn reservation_for(active: bool, measured_bytes: i64, segment_limit: i64) -> i64 {
    if active {
        segment_limit.max(measured_bytes)
    } else {
        measured_bytes
    }
}

/// Admission test against the configured total budget.
pub fn admission_allowed(total_reserved: i64, new_reservation: i64, max_total: i64) -> bool {
    total_reserved.saturating_add(new_reservation) <= max_total
}

/// One row of the capacity usage snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct UsageSnapshot {
    /// Catalog estimate of the physical size of the four saved_* tables.
    pub saved_bytes: i64,
    /// Starred segments still accepting packets (each reserves a full
    /// segment limit of remaining growth).
    pub active_starred: i64,
    /// Payload bytes of closed starred segments not yet fully archived.
    pub closed_unarchived_bytes: i64,
    /// Payload bytes of saved content awaiting post-unstar deletion.
    pub pending_cleanup_bytes: i64,
    pub starred_streams: i64,
    pub pending_cleanup_streams: i64,
}

impl UsageSnapshot {
    /// Total reservation claimed against favorite_max_total_bytes. The
    /// components may overlap briefly during archival (saved copy exists
    /// while the closed segment still counts as unarchived); the estimate
    /// deliberately errs high, never below the real footprint.
    pub fn reserved_bytes(&self, segment_limit: i64) -> i64 {
        self.saved_bytes
            .saturating_add(self.active_starred.saturating_mul(segment_limit))
            .saturating_add(self.closed_unarchived_bytes)
            .saturating_add(self.pending_cleanup_bytes)
    }
}

impl FavoriteService {
    pub(crate) fn new(pgpool: PgPool) -> Self {
        FavoriteService { pgpool }
    }

    /// Usage snapshot over the current favorite population. `exclude`
    /// omits one stream from the pending-cleanup sum (a re-star must not
    /// pay twice for content that is becoming its own archived base).
    async fn usage(&self, exclude: Option<i64>) -> Result<UsageSnapshot, sqlx::Error> {
        let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
            r#"
            SELECT
                (SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
                 FROM pg_class c
                 WHERE c.relkind = 'r'
                   AND c.relname IN ('saved_packet', 'saved_tag',
                                     'saved_http_message', 'saved_grpc_message')),
                (SELECT COUNT(*) FROM stream WHERE favorite AND active),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE favorite AND NOT active AND retention_state <> 'archived'),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE NOT favorite AND retention_state IN ('archiving', 'archived')
                   AND id <> COALESCE($1, -1)),
                (SELECT COUNT(*) FROM stream WHERE favorite),
                (SELECT COUNT(*) FROM stream
                 WHERE NOT favorite AND retention_state IN ('archiving', 'archived'))
            "#,
        )
        .bind(exclude)
        .fetch_one(&self.pgpool)
        .await?;
        Ok(UsageSnapshot {
            saved_bytes: row.0,
            active_starred: row.1,
            closed_unarchived_bytes: row.2,
            pending_cleanup_bytes: row.3,
            starred_streams: row.4,
            pending_cleanup_streams: row.5,
        })
    }

    pub async fn usage_response(
        &self,
        limits: &FavoriteLimits,
    ) -> Result<FavoriteUsageResponse, sqlx::Error> {
        let usage = self.usage(None).await?;
        Ok(FavoriteUsageResponse {
            max_total_bytes: limits.max_total_bytes,
            segment_bytes: limits.segment_bytes,
            disk_reserve_bytes: limits.disk_reserve_bytes,
            saved_bytes: usage.saved_bytes,
            reserved_bytes: usage.reserved_bytes(limits.segment_bytes),
            starred_streams: usage.starred_streams,
            active_starred_streams: usage.active_starred,
            pending_cleanup_streams: usage.pending_cleanup_streams,
        })
    }

    /// Star operation: lock the row, measure, reserve, commit the favorite
    /// state before any archive work is scheduled. Idempotent for an
    /// already-starred stream; a failed admission leaves the stream
    /// untouched.
    pub async fn star(
        &self,
        stream_id: i64,
        limits: &FavoriteLimits,
    ) -> Result<StarOutcome, sqlx::Error> {
        let mut tx = self.pgpool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(FAVORITE_LOCK_KEY.0)
            .bind(FAVORITE_LOCK_KEY.1)
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query_as::<_, FavoriteRow>(
            r#"
            SELECT favorite, active, retention_state, favorite_revision,
                   retention_bytes, archived_through_packet_id, segment_index, archive_error
            FROM stream
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

        if row.favorite {
            tx.commit().await?;
            return Ok(StarOutcome::Unchanged(row.to_response(stream_id)));
        }

        // Measured content: live payload bytes are the dominant term; tag
        // and L7 rows are two orders smaller and folded into the
        // reservation margin. Estimate documented per the design.
        let measured: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(octet_length(packet_data)), 0) FROM packets WHERE stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE stream SET retention_bytes = GREATEST(retention_bytes, $2) WHERE id = $1",
        )
        .bind(stream_id)
        .bind(measured)
        .execute(&mut *tx)
        .await?;

        let usage = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
            SELECT
                (SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
                 FROM pg_class c
                 WHERE c.relkind = 'r'
                   AND c.relname IN ('saved_packet', 'saved_tag',
                                     'saved_http_message', 'saved_grpc_message')),
                (SELECT COUNT(*) FROM stream WHERE favorite AND active),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE favorite AND NOT active AND retention_state <> 'archived'),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE NOT favorite AND retention_state IN ('archiving', 'archived')
                   AND id <> $1)
            "#,
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?;
        // This stream reserves a full segment limit on top of everyone
        // else's active reservations; its own pending-cleanup bytes are
        // excluded (they become its archived base, not new storage).
        let snapshot = UsageSnapshot {
            saved_bytes: usage.0,
            active_starred: usage.1,
            closed_unarchived_bytes: usage.2,
            pending_cleanup_bytes: usage.3,
            starred_streams: 0,
            pending_cleanup_streams: 0,
        };
        let total_reserved = snapshot.reserved_bytes(limits.segment_bytes);
        let reservation = reservation_for(row.active, measured, limits.segment_bytes);
        if !admission_allowed(total_reserved, reservation, limits.max_total_bytes) {
            tx.commit().await?;
            metrics::FAVORITE_STAR_REJECTIONS.inc();
            return Ok(StarOutcome::CapacityExceeded {
                total_reserved,
                reservation,
                max_total: limits.max_total_bytes,
            });
        }

        let updated = sqlx::query_as::<_, (i64, String)>(
            r#"
            UPDATE stream
            SET favorite = true,
                favorite_since = now(),
                favorite_revision = favorite_revision + 1,
                retention_state = 'archiving',
                archive_error = NULL
            WHERE id = $1
            RETURNING favorite_revision, retention_state
            "#,
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(StarOutcome::Started(FavoriteStateResponse {
            id: stream_id,
            favorite: true,
            favorite_revision: updated.0,
            retention_state: updated.1,
            segment_index: row.segment_index,
            retention_bytes: row.retention_bytes.max(measured),
            archived_through_packet_id: row.archived_through_packet_id,
            archive_error: None,
        }))
    }

    /// Unstar: clear the flag and bump the revision; live rows keep their
    /// normal retention and saved rows become pending bounded cleanup.
    pub async fn unstar(&self, stream_id: i64) -> Result<FavoriteStateResponse, sqlx::Error> {
        let mut tx = self.pgpool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(FAVORITE_LOCK_KEY.0)
            .bind(FAVORITE_LOCK_KEY.1)
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query_as::<_, FavoriteRow>(
            r#"
            SELECT favorite, active, retention_state, favorite_revision,
                   retention_bytes, archived_through_packet_id, segment_index, archive_error
            FROM stream
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

        if row.favorite {
            sqlx::query(
                "UPDATE stream SET favorite = false, favorite_revision = favorite_revision + 1
                 WHERE id = $1",
            )
            .bind(stream_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(FavoriteStateResponse {
            favorite: false,
            favorite_revision: if row.favorite {
                row.favorite_revision + 1
            } else {
                row.favorite_revision
            },
            ..row.to_response(stream_id)
        })
    }
}

/// Validated favorite limits resolved from app_settings (env-seeded).
#[derive(Debug, Clone, Copy)]
pub struct FavoriteLimits {
    pub max_total_bytes: i64,
    pub segment_bytes: i64,
    pub disk_reserve_bytes: i64,
}

fn parse_setting(raw: Option<String>, fallback: i64) -> i64 {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

impl FavoriteLimits {
    /// Built-in defaults (also the env seed values); used where reading
    /// settings per call would be wasteful or impossible.
    pub fn builtins() -> Self {
        FavoriteLimits {
            max_total_bytes: 10_737_418_240,
            segment_bytes: 268_435_456,
            disk_reserve_bytes: 2_147_483_648,
        }
    }

    pub async fn load(pool: &PgPool, config: &ApplicationConfig) -> Self {
        let max = crate::api::settings::get_setting(pool, SETTING_MAX_TOTAL).await;
        let segment = crate::api::settings::get_setting(pool, SETTING_SEGMENT).await;
        let reserve = crate::api::settings::get_setting(pool, SETTING_DISK_RESERVE).await;
        FavoriteLimits {
            max_total_bytes: parse_setting(max, config.favorite_max_total_bytes),
            segment_bytes: parse_setting(segment, config.favorite_segment_bytes),
            disk_reserve_bytes: parse_setting(reserve, config.favorite_disk_reserve_bytes),
        }
    }
}

/// Seed the three settings keys from env defaults. INSERT .. DO NOTHING
/// keeps operator overrides committed through the settings API.
pub async fn seed_favorite_settings(pool: &PgPool, config: &ApplicationConfig) {
    let rows = [
        (SETTING_MAX_TOTAL, config.favorite_max_total_bytes),
        (SETTING_SEGMENT, config.favorite_segment_bytes),
        (SETTING_DISK_RESERVE, config.favorite_disk_reserve_bytes),
    ];
    for (key, value) in rows {
        if let Err(e) = sqlx::query(
            "INSERT INTO app_settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
        )
        .bind(key)
        .bind(value.to_string())
        .execute(pool)
        .await
        {
            warn!("Failed to seed setting {}: {}", key, e);
        }
    }
}

/// Archive worker + saved-content cleanup worker + legacy favorite
/// migration. All loops are bounded and failure-contained: an error never
/// stops ingestion and never clears a star.
pub fn spawn_retention_workers(pool: PgPool) {
    tokio::spawn(archive_worker_loop(pool.clone()));
    tokio::spawn(saved_cleanup_worker_loop(pool));
}

/// Bounded startup migration of pre-feature favorite rows: rows with
/// content become archiving (the archive worker takes over), closed rows
/// with no content become incomplete historical shells. Idempotent; no
/// synthetic saved rows are created.
pub async fn migrate_legacy_favorites(pool: &PgPool) {
    let started = Instant::now();
    let mut archived = 0i64;
    let mut incomplete = 0i64;
    // Bounded: a large legacy population converges over repeated startup
    // ticks; a missing pass resumes on the next restart.
    for _ in 0..100 {
        let r1 = sqlx::query(
            r#"
            UPDATE stream s SET retention_state = 'archiving'
            WHERE s.id IN (
                SELECT s2.id FROM stream s2
                WHERE s2.favorite AND s2.retention_state = 'live'
                  AND EXISTS (SELECT 1 FROM packets p WHERE p.stream_id = s2.id)
                LIMIT 500
            )
            "#,
        )
        .execute(pool)
        .await;
        let r2 = sqlx::query(
            r#"
            UPDATE stream s SET retention_state = 'archiving'
            WHERE s.id IN (
                SELECT id FROM stream
                WHERE favorite AND retention_state = 'live' AND active
                LIMIT 500
            )
            "#,
        )
        .execute(pool)
        .await;
        let r3 = sqlx::query(
            r#"
            UPDATE stream s SET retention_state = 'incomplete'
            WHERE s.id IN (
                SELECT id FROM stream
                WHERE favorite AND retention_state = 'live' AND NOT active
                  AND NOT EXISTS (SELECT 1 FROM packets p WHERE p.stream_id = stream.id)
                LIMIT 500
            )
            "#,
        )
        .execute(pool)
        .await;
        match (r1, r2, r3) {
            (Ok(a), Ok(b), Ok(c)) => {
                let total = (a.rows_affected() + b.rows_affected() + c.rows_affected()) as i64;
                archived += a.rows_affected() as i64 + b.rows_affected() as i64;
                incomplete += c.rows_affected() as i64;
                if total == 0 {
                    break;
                }
            }
            (e1, e2, e3) => {
                warn!(
                    "Legacy favorite migration pass failed: {:?} {:?} {:?} - retrying next restart",
                    e1.map(|r| r.rows_affected()),
                    e2.map(|r| r.rows_affected()),
                    e3.map(|r| r.rows_affected())
                );
                break;
            }
        }
    }
    if archived > 0 || incomplete > 0 {
        info!(
            "Migrated legacy favorites: {} to archiving, {} incomplete shells ({:?})",
            archived,
            incomplete,
            started.elapsed()
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ArchiveStep {
    Advanced { through_packet_id: i64 },
    Drained,
    NotStarred,
}

/// Archive one bounded batch (packets + tags + http + grpc + shared-set
/// revival + progress advance) in ONE transaction: archived_through_packet_id
/// only moves forward when every derived row of the batch is already saved.
async fn archive_stream_batch(pool: &PgPool, stream_id: i64) -> Result<ArchiveStep, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let row = sqlx::query_as::<_, (bool, i64, bool, i64, String)>(
        "SELECT favorite, favorite_revision, active, archived_through_packet_id, retention_state
         FROM stream WHERE id = $1 FOR UPDATE",
    )
    .bind(stream_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((favorite, revision, active, through, state)) = row else {
        tx.commit().await?;
        return Ok(ArchiveStep::NotStarred);
    };
    if !favorite || (state != "archiving") {
        tx.commit().await?;
        return Ok(ArchiveStep::NotStarred);
    }

    let batch = sqlx::query_as::<_, (i64, i64, i64, String, i64, Vec<u8>)>(
        r#"
        SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
        FROM packets
        WHERE stream_id = $1 AND id > $2
        ORDER BY id
        LIMIT $3
        "#,
    )
    .bind(stream_id)
    .bind(through)
    .bind(ARCHIVE_BATCH_ROWS)
    .fetch_all(&mut *tx)
    .await?;

    if batch.is_empty() {
        if !active {
            // Closed and fully drained: the segment is complete.
            sqlx::query(
                r#"
                UPDATE stream SET retention_state = 'archived', archive_error = NULL
                WHERE id = $1 AND favorite AND favorite_revision = $2 AND retention_state = 'archiving'
                "#,
            )
            .bind(stream_id)
            .bind(revision)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        return Ok(ArchiveStep::Drained);
    }

    let max_id = batch.iter().map(|r| r.0).max().unwrap_or(through);
    let ids: Vec<i64> = batch.iter().map(|r| r.0).collect();

    // Packets (6 cols x 1000 rows stays below the 65535 bind limit).
    {
        let mut q = String::from(
            "INSERT INTO saved_packet (id, timestamp, timestamp_microseconds, direction, stream_id, packet_data) VALUES ",
        );
        for i in 0..batch.len() {
            if i > 0 {
                q.push_str(", ");
            }
            let base = i * 6;
            q.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6
            ));
        }
        q.push_str(" ON CONFLICT (id) DO NOTHING");
        let mut query = sqlx::query(&q);
        for (id, ts, us, dir, sid, data) in &batch {
            query = query
                .bind(*id)
                .bind(*ts)
                .bind(*us)
                .bind(dir)
                .bind(*sid)
                .bind(data);
        }
        query.execute(&mut *tx).await?;
    }

    // Tags joined through (packet_id, timestamp) like every other reader.
    let tag_rows = sqlx::query_as::<_, (i64, i64, i64, i32, i32, i64)>(
        r#"
        SELECT t.id, t.packet_id, t.tag_id, t.start_index, t.end_index, t.timestamp
        FROM tag t
        JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
        WHERE p.stream_id = $1 AND p.id = ANY($2)
        "#,
    )
    .bind(stream_id)
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    if !tag_rows.is_empty() {
        let mut q = String::from(
            "INSERT INTO saved_tag (id, packet_id, tag_id, start_index, end_index, timestamp) VALUES ",
        );
        for i in 0..tag_rows.len() {
            if i > 0 {
                q.push_str(", ");
            }
            let base = i * 6;
            q.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6
            ));
        }
        q.push_str(" ON CONFLICT (id) DO NOTHING");
        let mut query = sqlx::query(&q);
        for (id, packet_id, tag_id, start, end, ts) in &tag_rows {
            query = query
                .bind(*id)
                .bind(*packet_id)
                .bind(*tag_id)
                .bind(*start)
                .bind(*end)
                .bind(*ts);
        }
        query.execute(&mut *tx).await?;
    }

    // HTTP messages + revival of their shared header sets (cleanup only
    // removes a set unreferenced by live AND saved messages; reviving from
    // the bytes read inside this same transaction closes the race).
    let http_rows = sqlx::query_as::<_, SavedHttpRow>(
        r#"
        SELECT id, stream_id, packet_id, is_request, method, path, status_code, http_version,
               content_type, content_length, body_offset, body_length, header_set_id, timestamp
        FROM http_message
        WHERE stream_id = $1 AND packet_id = ANY($2)
        "#,
    )
    .bind(stream_id)
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    if !http_rows.is_empty() {
        let header_ids: Vec<i64> = http_rows
            .iter()
            .filter_map(|r| r.header_set_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        let headers: Vec<(i64, i64, serde_json::Value)> = if header_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, (i64, i64, serde_json::Value)>(
                "SELECT id, hash, headers FROM header_set WHERE id = ANY($1)",
            )
            .bind(&header_ids)
            .fetch_all(&mut *tx)
            .await?
        };
        let mut q = String::from(
            "INSERT INTO saved_http_message (id, stream_id, packet_id, is_request, method, path, status_code, http_version, content_type, content_length, body_offset, body_length, header_set_id, timestamp) VALUES ",
        );
        for i in 0..http_rows.len() {
            if i > 0 {
                q.push_str(", ");
            }
            let base = i * 14;
            q.push('(');
            for c in 0..14 {
                if c > 0 {
                    q.push_str(", ");
                }
                q.push_str(&format!("${}", base + c + 1));
            }
            q.push(')');
        }
        q.push_str(" ON CONFLICT (id) DO NOTHING");
        let mut query = sqlx::query(&q);
        for r in &http_rows {
            query = query
                .bind(r.id)
                .bind(r.stream_id)
                .bind(r.packet_id)
                .bind(r.is_request)
                .bind(&r.method)
                .bind(&r.path)
                .bind(r.status_code)
                .bind(&r.http_version)
                .bind(&r.content_type)
                .bind(r.content_length)
                .bind(r.body_offset)
                .bind(r.body_length)
                .bind(r.header_set_id)
                .bind(r.timestamp);
        }
        query.execute(&mut *tx).await?;

        if !headers.is_empty() {
            let mut q = String::from("INSERT INTO header_set (id, hash, headers) VALUES ");
            for i in 0..headers.len() {
                if i > 0 {
                    q.push_str(", ");
                }
                let base = i * 3;
                q.push_str(&format!(
                    "(${}, ${}, ${}::jsonb)",
                    base + 1,
                    base + 2,
                    base + 3
                ));
            }
            // Revival: a concurrent orphan cleanup may have deleted the set
            // between our read and our write; DO UPDATE restores it.
            q.push_str(
                " ON CONFLICT (id) DO UPDATE SET hash = EXCLUDED.hash, headers = EXCLUDED.headers",
            );
            let mut query = sqlx::query(&q);
            for (id, hash, json) in &headers {
                query = query.bind(*id).bind(*hash).bind(json);
            }
            query.execute(&mut *tx).await?;
        }
    }

    // gRPC messages via their http_message parents of this batch + body
    // set revival (same shared-table race as headers).
    let grpc_rows = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            Option<String>,
            Option<String>,
            bool,
            Option<i64>,
            i64,
        ),
    >(
        r#"
        SELECT g.id, g.stream_id, g.http_message_id, g.service_name, g.method_name,
               g.is_request, g.body_set_id, g.timestamp
        FROM grpc_message g
        JOIN http_message hm ON hm.id = g.http_message_id AND hm.timestamp = g.timestamp
        WHERE hm.stream_id = $1 AND hm.packet_id = ANY($2)
        "#,
    )
    .bind(stream_id)
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    if !grpc_rows.is_empty() {
        let body_ids: Vec<i64> = grpc_rows
            .iter()
            .filter_map(|r| r.6)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        let bodies: Vec<(i64, i64, String)> = if body_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, (i64, i64, String)>(
                "SELECT id, hash, decoded_json FROM grpc_body_set WHERE id = ANY($1)",
            )
            .bind(&body_ids)
            .fetch_all(&mut *tx)
            .await?
        };
        let mut q = String::from(
            "INSERT INTO saved_grpc_message (id, stream_id, http_message_id, service_name, method_name, is_request, body_set_id, timestamp) VALUES ",
        );
        for i in 0..grpc_rows.len() {
            if i > 0 {
                q.push_str(", ");
            }
            let base = i * 8;
            q.push('(');
            for c in 0..8 {
                if c > 0 {
                    q.push_str(", ");
                }
                q.push_str(&format!("${}", base + c + 1));
            }
            q.push(')');
        }
        q.push_str(" ON CONFLICT (id) DO NOTHING");
        let mut query = sqlx::query(&q);
        for r in &grpc_rows {
            query = query
                .bind(r.0)
                .bind(r.1)
                .bind(r.2)
                .bind(&r.3)
                .bind(&r.4)
                .bind(r.5)
                .bind(r.6)
                .bind(r.7);
        }
        query.execute(&mut *tx).await?;

        if !bodies.is_empty() {
            let mut q = String::from("INSERT INTO grpc_body_set (id, hash, decoded_json) VALUES ");
            for i in 0..bodies.len() {
                if i > 0 {
                    q.push_str(", ");
                }
                let base = i * 3;
                q.push_str(&format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));
            }
            q.push_str(
                " ON CONFLICT (id) DO UPDATE SET hash = EXCLUDED.hash, decoded_json = EXCLUDED.decoded_json",
            );
            let mut query = sqlx::query(&q);
            for (id, hash, json) in &bodies {
                query = query.bind(*id).bind(*hash).bind(json);
            }
            query.execute(&mut *tx).await?;
        }
    }

    // Progress advances only if the stream was not re-starred/unstarred
    // meanwhile: a stale worker must never move a newer star's markers.
    sqlx::query(
        r#"
        UPDATE stream
        SET archived_through_packet_id = GREATEST(archived_through_packet_id, $2),
            archive_error = NULL
        WHERE id = $1 AND favorite AND favorite_revision = $3
        "#,
    )
    .bind(stream_id)
    .bind(max_id)
    .bind(revision)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(ArchiveStep::Advanced {
        through_packet_id: max_id,
    })
}

async fn archive_worker_loop(pool: PgPool) {
    use tokio::time::{Duration, sleep};
    // Per-stream bounded backoff: a failing stream retries later without
    // starving the others, and never clears its star.
    let mut backoff: HashMap<i64, (Instant, u64)> = HashMap::new();
    loop {
        sleep(Duration::from_millis(ARCHIVE_TICK_MS)).await;
        let candidates = match sqlx::query_as::<_, (i64,)>(
            "SELECT id FROM stream WHERE favorite AND retention_state = 'archiving' ORDER BY id LIMIT $1",
        )
        .bind(ARCHIVE_CANDIDATES_PER_TICK)
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                debug_or_warn_archive_poll(&e);
                continue;
            }
        };

        for (stream_id,) in &candidates {
            // Per-stream bounded backoff: skip until its retry window ends.
            if backoff
                .get(stream_id)
                .is_some_and(|(not_before, _)| Instant::now() < *not_before)
            {
                continue;
            }
            match archive_stream_batch(&pool, *stream_id).await {
                Ok(step) => {
                    backoff.remove(stream_id);
                    if let ArchiveStep::Advanced { through_packet_id } = step {
                        log::debug!(
                            "Archive stream {} advanced to packet {}",
                            stream_id,
                            through_packet_id
                        );
                    }
                }
                Err(e) => {
                    metrics::FAVORITE_ARCHIVE_FAILURES.inc();
                    let prev_wait = backoff.get(stream_id).map(|(_, w)| *w).unwrap_or(1000);
                    let wait = (prev_wait * 2).min(ARCHIVE_BACKOFF_MAX_MS);
                    backoff.insert(
                        *stream_id,
                        (Instant::now() + Duration::from_millis(wait), wait),
                    );
                    error!(
                        "Archive batch failed for stream {} (retry in {} ms): {}",
                        stream_id, wait, e
                    );
                    // Surface the failure on the stream row for operators;
                    // best effort, never fatal, ids only - no payloads.
                    let _ = sqlx::query(
                        "UPDATE stream SET archive_error = $2 WHERE id = $1 AND favorite",
                    )
                    .bind(stream_id)
                    .bind(truncate_error(&e.to_string()))
                    .execute(&pool)
                    .await;
                }
            }
        }
        // Backoff entries for streams that stopped being starred would grow
        // without bound over a long game; prune the map on idle ticks.
        if candidates.is_empty() && !backoff.is_empty() {
            backoff.retain(|_, (not_before, _)| Instant::now() < *not_before);
        }
    }
}

fn debug_or_warn_archive_poll(e: &sqlx::Error) {
    // Poll failures are expected during DB restarts; keep them at warn with
    // no payload content.
    log::warn!("Archive worker poll failed: {}", e);
}

fn truncate_error(msg: &str) -> String {
    msg.chars().take(300).collect()
}

/// Delete saved rows for a favorite revision in bounded batches. The
/// re-star guard is the fresh row check: once favorite is true again (any
/// revision), no further deletion happens for this stream this tick.
async fn cleanup_saved_batch(pool: &PgPool, stream_id: i64) -> Result<bool, sqlx::Error> {
    let row = sqlx::query_as::<_, (bool, i64, String)>(
        "SELECT favorite, favorite_revision, retention_state FROM stream WHERE id = $1",
    )
    .bind(stream_id)
    .fetch_optional(pool)
    .await?;
    let Some((favorite, revision, state)) = row else {
        return Ok(false);
    };
    if favorite || (state != "archiving" && state != "archived") {
        return Ok(false);
    }

    let mut deleted_any = false;
    for _ in 0..CLEANUP_BATCHES_PER_STREAM_PER_TICK {
        // Packets first (the bulk), then metadata tables; a crash between
        // batches just leaves the stream pending-cleanup for the next tick.
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM saved_packet WHERE stream_id = $1 LIMIT $2")
                .bind(stream_id)
                .bind(CLEANUP_BATCH_ROWS)
                .fetch_all(pool)
                .await?;
        if !ids.is_empty() {
            sqlx::query("DELETE FROM saved_packet WHERE id = ANY($1)")
                .bind(&ids)
                .execute(pool)
                .await?;
            deleted_any = true;
            continue;
        }

        let tag_ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM saved_tag WHERE stream_id = $1 LIMIT $2")
                .bind(stream_id)
                .bind(CLEANUP_BATCH_ROWS)
                .fetch_all(pool)
                .await?;
        if !tag_ids.is_empty() {
            sqlx::query("DELETE FROM saved_tag WHERE id = ANY($1)")
                .bind(&tag_ids)
                .execute(pool)
                .await?;
            deleted_any = true;
            continue;
        }

        let http_ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM saved_http_message WHERE stream_id = $1 LIMIT $2")
                .bind(stream_id)
                .bind(CLEANUP_BATCH_ROWS)
                .fetch_all(pool)
                .await?;
        if !http_ids.is_empty() {
            sqlx::query("DELETE FROM saved_http_message WHERE id = ANY($1)")
                .bind(&http_ids)
                .execute(pool)
                .await?;
            deleted_any = true;
            continue;
        }

        let grpc_ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM saved_grpc_message WHERE stream_id = $1 LIMIT $2")
                .bind(stream_id)
                .bind(CLEANUP_BATCH_ROWS)
                .fetch_all(pool)
                .await?;
        if !grpc_ids.is_empty() {
            sqlx::query("DELETE FROM saved_grpc_message WHERE id = ANY($1)")
                .bind(&grpc_ids)
                .execute(pool)
                .await?;
            deleted_any = true;
            continue;
        }
        break;
    }

    if !deleted_any {
        // All saved content gone: return the stream to live state so
        // ordinary orphan cleanup owns the row again. Revision guard: a
        // re-star between the checks above cannot be overwritten because
        // favorite would be true (checked again in the WHERE).
        sqlx::query(
            r#"
            UPDATE stream SET retention_state = 'live'
            WHERE id = $1 AND NOT favorite AND favorite_revision = $2
              AND retention_state IN ('archiving', 'archived')
              AND NOT EXISTS (SELECT 1 FROM saved_packet WHERE stream_id = stream.id)
              AND NOT EXISTS (SELECT 1 FROM saved_tag WHERE stream_id = stream.id)
              AND NOT EXISTS (SELECT 1 FROM saved_http_message WHERE stream_id = stream.id)
              AND NOT EXISTS (SELECT 1 FROM saved_grpc_message WHERE stream_id = stream.id)
            "#,
        )
        .bind(stream_id)
        .bind(revision)
        .execute(pool)
        .await?;
    }
    Ok(deleted_any)
}

async fn saved_cleanup_worker_loop(pool: PgPool) {
    use tokio::time::{Duration, sleep};
    loop {
        sleep(Duration::from_millis(CLEANUP_TICK_MS)).await;
        let candidates = match sqlx::query_as::<_, (i64,)>(
            r#"
            SELECT id FROM stream
            WHERE NOT favorite AND retention_state IN ('archiving', 'archived')
            ORDER BY id
            LIMIT $1
            "#,
        )
        .bind(CLEANUP_CANDIDATES_PER_TICK)
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                log::warn!("Saved-cleanup poll failed: {}", e);
                continue;
            }
        };
        for (stream_id,) in candidates {
            match cleanup_saved_batch(&pool, stream_id).await {
                Ok(_) => {}
                Err(e) => {
                    metrics::FAVORITE_CLEANUP_FAILURES.inc();
                    log::warn!("Saved cleanup for stream {} failed: {}", stream_id, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_segment_reserves_full_limit() {
        // Worst case: the whole segment fills after the star.
        assert_eq!(reservation_for(true, 0, 268_435_456), 268_435_456);
        assert_eq!(
            reservation_for(true, 1000, 268_435_456),
            268_435_456,
            "measured content is inside the segment limit"
        );
    }

    #[test]
    fn measured_above_limit_still_bounded_by_measurement() {
        // Over-limit active segment (rotation lag): the estimate must not
        // shrink below what is already stored.
        assert_eq!(reservation_for(true, 500_000_000, 268_435_456), 500_000_000);
    }

    #[test]
    fn closed_segment_reserves_measured_size() {
        assert_eq!(reservation_for(false, 42, 268_435_456), 42);
        assert_eq!(reservation_for(false, 0, 268_435_456), 0);
    }

    #[test]
    fn admission_rejects_overflow_without_panicking() {
        let max = 1000i64;
        assert!(admission_allowed(600, 400, max));
        assert!(!admission_allowed(601, 400, max));
        assert!(!admission_allowed(i64::MAX, 1, max), "saturating, no panic");
        assert!(admission_allowed(0, max, max), "exactly full is allowed");
    }

    #[test]
    fn reserved_bytes_sums_all_components_with_saturation() {
        let usage = UsageSnapshot {
            saved_bytes: 10,
            active_starred: 2,
            closed_unarchived_bytes: 30,
            pending_cleanup_bytes: 40,
            starred_streams: 2,
            pending_cleanup_streams: 1,
        };
        assert_eq!(usage.reserved_bytes(100), 10 + 200 + 30 + 40);
        let huge = UsageSnapshot {
            saved_bytes: i64::MAX,
            ..usage
        };
        assert_eq!(huge.reserved_bytes(i64::MAX), i64::MAX);
    }

    #[test]
    fn settings_parse_rejects_zero_negative_and_garbage() {
        assert_eq!(parse_setting(Some("1024".into()), 7), 1024);
        assert_eq!(parse_setting(Some(" 1024 ".into()), 7), 1024);
        assert_eq!(parse_setting(Some("0".into()), 7), 7, "zero falls back");
        assert_eq!(parse_setting(Some("-5".into()), 7), 7);
        assert_eq!(parse_setting(Some("abc".into()), 7), 7);
        assert_eq!(parse_setting(None, 7), 7);
    }

    #[test]
    fn truncate_error_caps_length() {
        let long = "x".repeat(1000);
        assert_eq!(truncate_error(&long).len(), 300);
        assert_eq!(truncate_error("short"), "short");
    }
}
