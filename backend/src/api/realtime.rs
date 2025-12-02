use actix_web::{Error, HttpRequest, HttpResponse, get, web};
use actix_ws::Message;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;

pub type BroadcastSender = Arc<broadcast::Sender<String>>;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(echo);
}

// WebSocket handler
#[get("/ws")]
async fn echo(
    req: HttpRequest,
    stream: web::Payload,
    broadcaster: web::Data<BroadcastSender>,
) -> Result<HttpResponse, Error> {
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    
    let mut rx = broadcaster.subscribe();
    
    // Spawn task to send messages from broadcaster to this WebSocket session
    let mut session_sender = session.clone();
    actix_web::rt::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if session_sender.text(msg).await.is_err() {
                break;
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
