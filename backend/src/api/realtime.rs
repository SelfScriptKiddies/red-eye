use actix::ActorContext;
use actix::AsyncContext;
use actix::fut::WrapFuture;
use actix::prelude::*;
use actix::{Actor, Context, Handler, Message, Recipient, StreamHandler};
use actix_web::{Error, HttpRequest, HttpResponse, Responder, get, post, web};
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use utoipa::ToSchema;

/// -------------------------
/// DTO
/// -------------------------
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BroadcastRequest {
    pub message: String,
}

/// -------------------------
/// Service (Actor)
/// -------------------------

/// Сообщение для рассылки всем клиентам
#[derive(Message)]
#[rtype(result = "()")]
pub struct BroadcastMessage(pub String);

/// Сообщение о подключении нового клиента
#[derive(Message)]
#[rtype(usize)]
pub struct Connect {
    pub addr: Recipient<BroadcastMessage>,
}

/// Сообщение об отключении клиента
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
}

/// Актор, управляющий всеми подключениями
pub struct RealtimeService {
    sessions: HashMap<usize, Recipient<BroadcastMessage>>,
    next_id: usize,
}

impl RealtimeService {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            next_id: 1,
        }
    }

    fn broadcast(&self, message: &str) {
        for (_, addr) in &self.sessions {
            let _ = addr.do_send(BroadcastMessage(message.to_string()));
        }
    }
}

impl Actor for RealtimeService {
    type Context = Context<Self>;
}

impl Handler<Connect> for RealtimeService {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        let id = self.next_id;
        self.next_id += 1;
        self.sessions.insert(id, msg.addr);
        log::info!("Client connected: {}", id);
        id
    }
}

impl Handler<Disconnect> for RealtimeService {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        self.sessions.remove(&msg.id);
        log::info!("Client disconnected: {}", msg.id);
    }
}

impl Handler<BroadcastMessage> for RealtimeService {
    type Result = ();

    fn handle(&mut self, msg: BroadcastMessage, _: &mut Context<Self>) {
        self.broadcast(&msg.0);
    }
}

/// -------------------------
/// WebSocket Session
/// -------------------------
struct RealtimeSession {
    id: usize,
    hb: Instant,
    service_addr: actix::Addr<RealtimeService>,
}

impl RealtimeSession {
    fn new(service_addr: actix::Addr<RealtimeService>) -> Self {
        Self {
            id: 0,
            hb: Instant::now(),
            service_addr,
        }
    }

    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            if Instant::now().duration_since(act.hb) > Duration::from_secs(30) {
                log::warn!("Client {} heartbeat failed", act.id);
                act.service_addr.do_send(Disconnect { id: act.id });
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }
}

impl Actor for RealtimeSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address().recipient();
        let service_addr = self.service_addr.clone();

        service_addr
            .send(Connect { addr })
            .into_actor(self)
            .then(|res, act, ctx| {
                if let Ok(id) = res {
                    act.id = id;
                    ctx.text(format!("Connected with id {}", id));
                } else {
                    ctx.stop();
                }
                act.start_heartbeat(ctx);
                actix::fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> actix::Running {
        self.service_addr.do_send(Disconnect { id: self.id });
        actix::Running::Stop
    }
}

impl Handler<BroadcastMessage> for RealtimeSession {
    type Result = ();

    fn handle(&mut self, msg: BroadcastMessage, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for RealtimeSession {
    fn handle(
        &mut self,
        item: Result<ws::Message, ws::ProtocolError>,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        match item {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                self.service_addr
                    .do_send(BroadcastMessage(text.to_string()));
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => {}
        }
    }
}

/// -------------------------
/// HTTP Handlers
/// -------------------------
#[utoipa::path(
    get,
    path = "/api/realtime",
    responses(
        (status = 101, description = "WebSocket connection established"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "realtime"
)]
#[get("/realtime")]
async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    service: web::Data<actix::Addr<RealtimeService>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        RealtimeSession::new(service.get_ref().clone()),
        &req,
        stream,
    )
}

#[utoipa::path(
    post,
    path = "/api/realtime/broadcast",
    request_body = BroadcastRequest,
    responses(
        (status = 200, description = "Message broadcasted to all clients"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "realtime"
)]
#[post("/realtime/broadcast")]
async fn broadcast_route(
    service: web::Data<actix::Addr<RealtimeService>>,
    body: web::Json<BroadcastRequest>,
) -> impl Responder {
    service.do_send(BroadcastMessage(body.message.clone()));
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

/// -------------------------
/// Config
/// -------------------------
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(ws_route).service(broadcast_route);
}
