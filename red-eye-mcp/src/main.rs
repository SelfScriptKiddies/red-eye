mod audit;
mod auth;
mod backend;
mod config;
mod handler;
mod state;
mod streams;
mod tools;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use log::info;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

// red-eye-mcp: remote MCP Streamable HTTP interface for Red Eye.
// Read-only by default; full control requires BOTH the process opt-in
// (MCP_MODE + MCP_ALLOW_FULL_CONTROL) and a full_control token. The
// service holds no database credentials: everything flows through the
// Red Eye backend HTTP API.

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let config = match config::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            log::error!("{}", e);
            std::process::exit(1);
        }
    };
    info!(
        "red-eye-mcp starting: mode={:?} backend={} bind={} public_url={:?}",
        config.mode, config.backend_url, config.bind, config.public_url
    );

    let backend = match backend::BackendClient::new(&config) {
        Ok(b) => Arc::new(b),
        Err(e) => {
            log::error!("backend client init failed: {}", e);
            std::process::exit(1);
        }
    };

    let app_state = Arc::new(auth::AppState {
        backend: backend.clone(),
        rate_token: state::RateLimiter::per_token(),
        rate_addr: state::RateLimiter::per_addr(),
        sessions: state::SessionTracker::new(),
        permits: Arc::new(tokio::sync::Semaphore::new(config::MAX_CONCURRENT_CALLS)),
    });

    let cancellation = CancellationToken::new();
    let http_config =
        StreamableHttpServerConfig::default().with_cancellation_token(cancellation.child_token());

    let server = handler::SocMcpServer {
        backend: backend.clone(),
        dryruns: Arc::new(state::DryRunStore::new()),
        mode_read_only: config.mode == config::Mode::ReadOnly,
        public_url: config.public_url,
    };

    let service: StreamableHttpService<handler::SocMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(server.clone()),
            LocalSessionManager::default().into(),
            http_config,
        );

    let app = Router::new()
        .nest_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            auth::middleware,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(config::MAX_BODY_BYTES))
        .with_state(app_state);

    let listener = match tokio::net::TcpListener::bind(&config.bind).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("bind {} failed: {}", config.bind, e);
            std::process::exit(1);
        }
    };
    info!("listening on {}", config.bind);
    let _ = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(cancellation))
    .await;
}

async fn shutdown_signal(cancellation: CancellationToken) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    cancellation.cancel();
}
