use actix_web::{HttpResponse, get, web};
use log::{error, info, warn};
use serde::Serialize;
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    Healthy,
    Degraded,
    Critical,
}

pub struct HealthMonitor {
    window: Arc<RwLock<VecDeque<(Instant, Duration)>>>,
    state: Arc<RwLock<HealthState>>,
    healthy_since: Arc<RwLock<Option<Instant>>>,
    warn_ms: u64,
    critical_ms: u64,
    /// Total queries cancelled by the stuck-query watchdog (lifetime
    /// counter, reported as `stuck_queries_cancelled` in /api/health).
    stuck_queries_cancelled: AtomicU64,
}

impl HealthMonitor {
    pub fn new(warn_ms: u64, critical_ms: u64) -> Self {
        Self {
            window: Arc::new(RwLock::new(VecDeque::new())),
            state: Arc::new(RwLock::new(HealthState::Healthy)),
            healthy_since: Arc::new(RwLock::new(None)),
            warn_ms,
            critical_ms,
            stuck_queries_cancelled: AtomicU64::new(0),
        }
    }

    pub fn record(&self, latency: Duration) {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(60);

        let mut window = self.window.write().unwrap();
        window.push_back((now, latency));
        while window.front().map(|(t, _)| *t < cutoff).unwrap_or(false) {
            window.pop_front();
        }

        let p95 = Self::compute_p95(&window);
        drop(window);

        let new_state = if p95 >= self.critical_ms {
            HealthState::Critical
        } else if p95 >= self.warn_ms {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        let current = *self.state.read().unwrap();

        if new_state == HealthState::Healthy && current != HealthState::Healthy {
            let mut since = self.healthy_since.write().unwrap();
            match *since {
                Some(t) if t.elapsed() >= Duration::from_secs(10) => {
                    *self.state.write().unwrap() = HealthState::Healthy;
                    *since = None;
                    info!("Health recovered to Healthy (P95={}ms)", p95);
                }
                None => {
                    *since = Some(Instant::now());
                }
                _ => {}
            }
        } else {
            if new_state != current {
                match new_state {
                    HealthState::Degraded => warn!("Health degraded (P95={}ms)", p95),
                    HealthState::Critical => error!("Health CRITICAL (P95={}ms)", p95),
                    _ => {}
                }
            }
            *self.state.write().unwrap() = new_state;
            *self.healthy_since.write().unwrap() = None;
        }
    }

    fn compute_p95(window: &VecDeque<(Instant, Duration)>) -> u64 {
        if window.is_empty() {
            return 0;
        }
        let mut latencies: Vec<u64> = window.iter().map(|(_, d)| d.as_millis() as u64).collect();
        latencies.sort_unstable();
        let idx = ((latencies.len() as f64) * 0.95).ceil() as usize - 1;
        latencies[idx.min(latencies.len() - 1)]
    }

    pub fn state(&self) -> HealthState {
        *self.state.read().unwrap()
    }

    pub fn p95_ms(&self) -> u64 {
        let window = self.window.read().unwrap();
        Self::compute_p95(&window)
    }

    pub fn record_stuck_query_cancelled(&self) {
        self.stuck_queries_cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stuck_queries_cancelled(&self) -> u64 {
        self.stuck_queries_cancelled.load(Ordering::Relaxed)
    }
}

#[derive(Serialize)]
struct HealthResponse {
    state: HealthState,
    p95_ms: u64,
    pool_active: u32,
    pool_idle: u32,
    pool_max: u32,
    uptime_secs: u64,
    stuck_queries_cancelled: u64,
}

static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

pub fn init_start_time() {
    START_TIME.get_or_init(Instant::now);
}

#[get("/health")]
pub async fn health_endpoint(
    monitor: web::Data<Arc<HealthMonitor>>,
    pool: web::Data<PgPool>,
) -> HttpResponse {
    let uptime = START_TIME.get().map(|t| t.elapsed().as_secs()).unwrap_or(0);
    let resp = HealthResponse {
        state: monitor.state(),
        p95_ms: monitor.p95_ms(),
        pool_active: pool.size().saturating_sub(pool.num_idle() as u32),
        pool_idle: pool.num_idle() as u32,
        pool_max: pool.size(),
        uptime_secs: uptime,
        stuck_queries_cancelled: monitor.stuck_queries_cancelled(),
    };
    HttpResponse::Ok().json(resp)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(health_endpoint);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stuck_query_cancel_counter_counts_monotonically() {
        let monitor = HealthMonitor::new(1000, 2000);
        assert_eq!(monitor.stuck_queries_cancelled(), 0);
        monitor.record_stuck_query_cancelled();
        monitor.record_stuck_query_cancelled();
        assert_eq!(monitor.stuck_queries_cancelled(), 2);
    }
}
