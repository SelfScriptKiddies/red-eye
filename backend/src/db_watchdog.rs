//! Stuck-query watchdog — the live half of `HEALTH_KILL_QUERY_AFTER_MS`.
//!
//! `statement_timeout` (applied to every pooled connection in `db.rs`)
//! already bounds queries issued through this backend's pool. The watchdog
//! is the safety net for what slips past that mechanism: manual sessions
//! without a timeout (psql, migrations), lock waits behind
//! idle-in-transaction holders, and driver edge cases. It scans
//! `pg_stat_activity` on a fixed interval and cancels (never terminates —
//! `pg_cancel_backend` aborts only the current statement, the connection
//! survives) any active query older than the threshold, logging every hit
//! so operators see what was stuck. Total cancellations are exposed via
//! `/api/health` (`stuck_queries_cancelled`).
//!
//! The loop is strictly best-effort: any error (DB down, permission denied
//! on a foreign-role backend) is logged and the next tick proceeds. This
//! task must never panic or take the application down.

use crate::configuration::ApplicationConfig;
use crate::db;
use crate::health::HealthMonitor;
use log::{error, info, warn};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

/// How often to scan `pg_stat_activity` for a given kill threshold.
///
/// Half the threshold (so a stuck query is caught within ~1.5x the
/// threshold in the worst case), capped at 30s so a large threshold does
/// not make scans too rare to be useful, floored at 1s so a small
/// threshold (tight budgets, tests) cannot turn into a busy loop.
pub(crate) fn scan_interval(threshold_ms: u64) -> Duration {
    Duration::from_secs((threshold_ms / 2_000).clamp(1, 30))
}

/// Kill threshold in seconds (fractional), as expected by
/// `make_interval(secs => ...)`.
pub(crate) fn threshold_secs(threshold_ms: u64) -> f64 {
    threshold_ms as f64 / 1000.0
}

/// One watchdog iteration: find queries active beyond the threshold and
/// cancel them. Returns the number of stuck queries found.
///
/// Exclusions in the scan:
/// - `pid <> pg_backend_pid()` — this scan itself;
/// - `query NOT LIKE '%pg_stat_activity%'` — watchdog scans from other
///   backend instances and humans inspecting activity views (never kill
///   the observer);
/// - `backend_type = 'client backend'` — never touch autovacuum, walsender
///   or parallel workers (cancelling the leader covers its workers).
pub(crate) async fn scan_once(
    pool: &PgPool,
    threshold_ms: u64,
    monitor: &HealthMonitor,
) -> Result<usize, sqlx::Error> {
    let stuck: Vec<(i32, f64, Option<String>)> = sqlx::query_as(
        r#"
        SELECT pid,
               EXTRACT(EPOCH FROM (now() - query_start))::float8 AS runtime_secs,
               left(query, 120) AS query_snippet
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
          AND state = 'active'
          AND query_start IS NOT NULL
          AND query NOT LIKE '%pg_stat_activity%'
          AND now() - query_start > make_interval(secs => $1)
        "#,
    )
    .bind(threshold_secs(threshold_ms))
    .fetch_all(pool)
    .await?;

    let found = stuck.len();

    for (pid, runtime_secs, snippet) in stuck {
        let snippet = snippet.unwrap_or_default();
        match sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .fetch_one(pool)
            .await
        {
            // Cancel aborted the statement; the backend and its pooled
            // connection survive.
            Ok(true) => {
                monitor.record_stuck_query_cancelled();
                warn!(
                    "Stuck-query watchdog cancelled pid {} after {:.1}s: {}",
                    pid, runtime_secs, snippet
                );
            }
            // Query finished on its own between the scan and the cancel.
            Ok(false) => info!(
                "Stuck-query watchdog: pid {} ({:.1}s: {}) finished before cancel",
                pid, runtime_secs, snippet
            ),
            // E.g. permission denied for a backend owned by another role.
            Err(e) => warn!(
                "Stuck-query watchdog could not cancel pid {} ({:.1}s: {}): {}",
                pid, runtime_secs, snippet, e
            ),
        }
    }

    Ok(found)
}

/// Spawn the watchdog background loop (called once at startup).
///
/// `HEALTH_KILL_QUERY_AFTER_MS = 0` disables the watchdog entirely; any
/// other missing/invalid value falls back to the `configuration.rs`
/// default (30 000 ms).
pub fn spawn(config: &ApplicationConfig, monitor: Arc<HealthMonitor>) {
    let threshold_ms = config.health_kill_query_after_ms;
    if threshold_ms == 0 {
        info!("Stuck-query watchdog disabled (HEALTH_KILL_QUERY_AFTER_MS=0)");
        return;
    }

    // Dedicated single lazy connection instead of the shared pool: pool
    // exhaustion is precisely the situation in which queries get stuck, so
    // the watchdog must not queue behind 50 acquires to do its job.
    // Lazy: a temporarily unreachable DB must not fail startup.
    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy(db::database_url(config).as_str())
    {
        Ok(pool) => pool,
        Err(e) => {
            // The URL was already validated by establish_connection at
            // startup, so this is practically unreachable; still, never
            // take the app down — just run without the watchdog.
            error!("Stuck-query watchdog could not parse database URL, not started: {}", e);
            return;
        }
    };

    let interval = scan_interval(threshold_ms);
    info!(
        "Stuck-query watchdog enabled: cancelling queries active > {}ms, scanning every {:?}",
        threshold_ms, interval
    );

    tokio::spawn(async move {
        use tokio::time::MissedTickBehavior;
        let mut ticker = tokio::time::interval(interval);
        // If a tick overruns (slow DB), don't fire a burst of catch-up
        // scans — resynchronize to the next period boundary.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(e) = scan_once(&pool, threshold_ms, &monitor).await {
                error!("Stuck-query watchdog scan failed: {}", e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_interval_is_half_the_threshold() {
        assert_eq!(scan_interval(30_000), Duration::from_secs(15));
        assert_eq!(scan_interval(60_000), Duration::from_secs(30));
    }

    #[test]
    fn scan_interval_capped_at_30s() {
        assert_eq!(scan_interval(120_000), Duration::from_secs(30));
        assert_eq!(scan_interval(3_600_000), Duration::from_secs(30));
    }

    #[test]
    fn scan_interval_floored_at_1s() {
        assert_eq!(scan_interval(2_000), Duration::from_secs(1));
        assert_eq!(scan_interval(1_999), Duration::from_secs(1));
        assert_eq!(scan_interval(500), Duration::from_secs(1));
        // Disabled (0) never reaches the ticker, but the function must
        // still return a sane non-zero interval.
        assert_eq!(scan_interval(0), Duration::from_secs(1));
    }

    #[test]
    fn threshold_secs_converts_ms_to_fractional_seconds() {
        assert_eq!(threshold_secs(30_000), 30.0);
        assert_eq!(threshold_secs(1_500), 1.5);
        assert_eq!(threshold_secs(1), 0.001);
        assert_eq!(threshold_secs(0), 0.0);
    }
}

/// Live integration check against a scratch Postgres (not run by default):
///
/// ```sh
/// docker run --rm -d --name killq-test \
///   -e POSTGRES_PASSWORD=t -p 55433:5432 postgres:16-alpine
/// cargo test -p backend --lib -- --ignored watchdog_live
/// docker rm -f killq-test
/// ```
///
/// Override the target with `KILLQ_TEST_DATABASE_URL`.
#[cfg(test)]
mod live_tests {
    use super::*;
    use sqlx::Connection;
    use std::time::Instant;

    fn test_url() -> String {
        std::env::var("KILLQ_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:t@127.0.0.1:55433/postgres".to_string())
    }

    #[tokio::test]
    #[ignore]
    async fn watchdog_live_cancels_stuck_pg_sleep() {
        let url = test_url();
        let watchdog_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect watchdog pool");

        let monitor = HealthMonitor::new(1, 2);

        // Victim session on its own connection: a 60s sleep with no
        // statement_timeout, i.e. exactly the kind of query only the
        // watchdog can stop.
        let mut victim = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("connect victim session");
        let victim_task =
            tokio::spawn(
                async move { sqlx::query("SELECT pg_sleep(60)").execute(&mut victim).await },
            );

        // Wait past the 1s threshold so the sleep registers as stuck.
        tokio::time::sleep(Duration::from_millis(1_600)).await;

        let started = Instant::now();
        let found = scan_once(&watchdog_pool, 1_000, &monitor)
            .await
            .expect("scan_once");
        assert!(found >= 1, "expected at least one stuck query, found {found}");

        // The victim must observe a cancellation error quickly — not wait
        // out the remaining ~58s of pg_sleep.
        let outcome: Result<sqlx::postgres::PgQueryResult, sqlx::Error> =
            tokio::time::timeout(Duration::from_secs(10), victim_task)
                .await
                .expect("victim query should finish within 10s")
                .expect("victim task joined");
        let elapsed = started.elapsed();
        assert!(
            outcome.is_err(),
            "pg_sleep should have been cancelled, got {outcome:?}"
        );
        let err = outcome.unwrap_err().to_string().to_lowercase();
        assert!(
            err.contains("cancel"),
            "expected a cancel error, got: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "cancel took too long: {elapsed:?}"
        );

        assert!(
            monitor.stuck_queries_cancelled() >= 1,
            "cancellation counter should have counted the kill"
        );
    }
}
