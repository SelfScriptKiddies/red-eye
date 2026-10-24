use actix_identity::Identity;
use actix_web::{Error, HttpRequest, HttpResponse, get, web};
use actix_ws::Message;
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::configuration::ApplicationConfig;

pub type BroadcastSender = Arc<broadcast::Sender<String>>;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(ws_handler);
}

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

// WebSocket handler — requires session cookie or ?token=<web_password>
#[get("/ws")]
async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    broadcaster: web::Data<BroadcastSender>,
    identity: Option<Identity>,
    query: web::Query<WsQuery>,
    app_config: web::Data<ApplicationConfig>,
) -> Result<HttpResponse, Error> {
    // Auth: accept session cookie or token query parameter
    let authenticated =
        identity.is_some() || query.token.as_deref() == Some(&app_config.web_password);

    if !authenticated {
        return Ok(HttpResponse::Unauthorized().json(
            serde_json::json!({"error": "WebSocket requires authentication. Use session cookie or ?token=<password>"}),
        ));
    }

    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    let mut rx = broadcaster.subscribe();

    // Spawn task to send messages from broadcaster to this WebSocket session
    let mut session_sender = session.clone();
    actix_web::rt::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    if session_sender.text(msg).await.is_err() {
                        break;
                    }
                }
                // A momentarily-slow client falls behind the broadcast ring buffer.
                // Skip the missed messages but keep the connection alive.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("WebSocket client lagged, skipped {} broadcast messages", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Handle incoming messages (ping/pong, close, etc.) in main task
    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }
    });

    Ok(res)
}
