mod backend;
mod config;
mod llm;
mod mcp_client;
mod playbook;
mod prompts;
mod result;

use std::time::Duration;
use tokio::time::sleep;

use backend::{Backend, ClaimOutcome, JobError};
use config::Config;
use llm::LlmClient;

// LLM-SOC triage worker: claims jobs from the Red Eye backend, gathers
// evidence through the read-only MCP service, runs the FIXED playbook
// against a self-hosted OpenAI-compatible LLM, submits typed results.
// No PostgreSQL or NATS credentials; no mutation capability anywhere.

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            log::error!("{}", e);
            std::process::exit(1);
        }
    };
    let backend = match Backend::new(&config.backend_url, &config.worker_token) {
        Ok(b) => std::sync::Arc::new(b),
        Err(e) => {
            log::error!("backend client init failed: {}", e);
            std::process::exit(1);
        }
    };
    let llm = match LlmClient::new() {
        Ok(l) => std::sync::Arc::new(l),
        Err(e) => {
            log::error!("llm client init failed: {}", e);
            std::process::exit(1);
        }
    };

    // Initial settings load decides the concurrency fan-out (default 1).
    let cfg = match backend.worker_config().await {
        Ok(c) => c,
        Err(_) => backend::WorkerConfig {
            enabled: false,
            base_url: String::new(),
            api_key: String::new(),
            model: String::new(),
            prompt_version: prompts::PROMPT_VERSION,
            settings_generation: 0,
            max_context_tokens: 16384,
            max_output_tokens: 1024,
            max_tool_calls: 8,
            max_tool_result_bytes: 16384,
            job_timeout_secs: 120,
            max_attempts: 3,
            concurrency: 1,
        },
    };
    let workers = cfg.concurrency.clamp(1, 32) as usize;
    log::info!(
        "soc-worker starting: worker_id={} concurrency={} backend={} mcp={}",
        config.worker_id,
        workers,
        config.backend_url,
        config.mcp_url
    );

    let mut handles = Vec::with_capacity(workers);
    for slot in 0..workers {
        let backend = backend.clone();
        let llm = llm.clone();
        let mut config = config.clone();
        if slot > 0 {
            config.worker_id = format!("{}-{}", config.worker_id, slot);
        }
        handles.push(tokio::spawn(worker_loop(backend, llm, config)));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn worker_loop(
    backend: std::sync::Arc<Backend>,
    llm: std::sync::Arc<LlmClient>,
    config: Config,
) {
    let mut backoff_secs: u64 = 1;
    loop {
        // Runtime settings every iteration: operator changes apply
        // without restarts.
        let cfg = match backend.worker_config().await {
            Ok(c) => c,
            Err(JobError::Permanent(m)) => {
                log::error!("worker-config rejected permanently: {}; pausing 60s", m);
                sleep(Duration::from_secs(60)).await;
                continue;
            }
            Err(JobError::Transient(m)) => {
                log::warn!(
                    "worker-config unavailable ({}); retry in {}s",
                    m,
                    backoff_secs
                );
                sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        };
        backoff_secs = 1;

        if !cfg.enabled {
            // Explicit pause: visible, quiet, no tight loop.
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        match backend.claim(&config.worker_id, cfg.job_timeout_secs).await {
            ClaimOutcome::Empty => {
                sleep(Duration::from_secs(2)).await;
            }
            ClaimOutcome::Paused(reason) => {
                log::info!("claims paused: {}", reason);
                sleep(Duration::from_secs(15)).await;
            }
            ClaimOutcome::Failed(JobError::Transient(m)) => {
                log::warn!("claim failed ({}); retry in {}s", m, backoff_secs);
                sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
            ClaimOutcome::Failed(JobError::Permanent(m)) => {
                log::error!("claim rejected permanently: {}; pausing 60s", m);
                sleep(Duration::from_secs(60)).await;
            }
            ClaimOutcome::Job(job) => {
                log::info!(
                    "claimed job {} stream {} rev {} (attempt {}, trigger {})",
                    job.id,
                    job.stream_id,
                    job.stream_revision,
                    job.attempts,
                    job.trigger
                );
                // Lease-bounded attempt: exceeding the lease hands the
                // job to another worker; our late submit is rejected by
                // the backend.
                let timeout = Duration::from_secs(job.lease_secs.max(10).saturating_sub(5) as u64);
                let outcome = tokio::time::timeout(
                    timeout,
                    playbook::run_attempt(
                        &backend,
                        &llm,
                        &job,
                        &cfg,
                        &config.mcp_url,
                        &config.mcp_token,
                    ),
                )
                .await;
                match outcome {
                    Ok(playbook::AttemptOutcome {
                        submitted: true, ..
                    }) => {
                        log::info!("job {} submitted", job.id);
                    }
                    Ok(playbook::AttemptOutcome {
                        submitted: false,
                        error: Some(e),
                    }) => {
                        log::warn!("job {} failed: {:?}", job.id, classify(&e));
                        let _ = backend.report_failure(job.id, &e).await;
                    }
                    Ok(playbook::AttemptOutcome {
                        submitted: false,
                        error: None,
                    }) => {}
                    Err(_elapsed) => {
                        log::warn!("job {} exceeded its lease budget", job.id);
                        let _ = backend
                            .report_failure(
                                job.id,
                                &JobError::Transient("attempt exceeded lease budget".to_string()),
                            )
                            .await;
                    }
                }
            }
        }
    }
}

fn classify(e: &JobError) -> &'static str {
    match e {
        JobError::Permanent(_) => "permanent",
        JobError::Transient(_) => "transient",
    }
}
