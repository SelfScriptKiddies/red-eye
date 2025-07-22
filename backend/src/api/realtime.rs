use actix::prelude::*;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, get, web};
use actix_web_actors::ws;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

pub fn config(cfg: &mut web::ServiceConfig) {
    let broadcaster = Broadcaster::new().start();
    let broadcaster_data = web::Data::new(broadcaster);
    cfg.service(ws_route)
        .service(broadcast)
        .app_data(broadcaster_data.clone());
}

// Message type for broadcasting
#[derive(Message)]
#[rtype(result = "()")]
struct BroadcastMessage {
    content: String,
}

// Message to register a client
#[derive(Message)]
#[rtype(result = "()")]
struct RegisterClient {
    addr: Addr<MyWebSocket>,
}

// Message to unregister a client
#[derive(Message)]
#[rtype(result = "()")]
struct UnregisterClient {
    id: i32,
}

// Broadcaster actor to manage all WebSocket clients
struct Broadcaster {
    clients: Arc<Mutex<HashMap<i32, Addr<MyWebSocket>>>>,
}

impl Broadcaster {
    fn new() -> Self {
        Broadcaster {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn broadcast(&self, message: &str) {
        let clients = self.clients.lock();
        for addr in clients.values() {
            addr.do_send(BroadcastMessage {
                content: message.to_string(),
            });
        }
    }
}

impl Actor for Broadcaster {
    type Context = Context<Self>;
}

impl Handler<RegisterClient> for Broadcaster {
    type Result = ();

    fn handle(&mut self, msg: RegisterClient, _: &mut Self::Context) {
        let mut clients = self.clients.lock();
        clients.insert(clients.len() + 1, msg.addr);
    }
}

impl Handler<UnregisterClient> for Broadcaster {
    type Result = ();

    fn handle(&mut self, msg: UnregisterClient, _: &mut Self::Context) {
        let mut clients = self.clients.lock();
        clients.remove(&msg.id);
    }
}

impl Handler<BroadcastMessage> for Broadcaster {
    type Result = ();

    fn handle(&mut self, msg: BroadcastMessage, _: &mut Self::Context) {
        self.broadcast(&msg.content);
    }
}

// WebSocket actor for each client
struct MyWebSocket {
    id: i32,
    broadcaster: Addr<Broadcaster>,
}

impl MyWebSocket {
    fn new(id: i32, broadcaster: Addr<Broadcaster>) -> Self {
        MyWebSocket { id, broadcaster }
    }
}

impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Register the client with the broadcaster
        self.broadcaster.do_send(RegisterClient {
            addr: ctx.address(),
        });
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        // Unregister the client when it disconnects
        self.broadcaster.do_send(UnregisterClient { id: self.id });
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                // Echo the message back to the client or broadcast it
                ctx.text(text.clone());
                self.broadcaster.do_send(BroadcastMessage {
                    content: text.to_string(),
                });
            }
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Pong(_)) => {}
            Ok(ws::Message::Close(_)) => ctx.stop(),
            _ => (),
        }
    }
}

// WebSocket route handler
#[get("/ws")]
async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    broadcaster: web::Data<Addr<Broadcaster>>,
) -> Result<HttpResponse, Error> {
    let id = rand::random::<i32>(); // Generate a unique ID for the client
    let ws = MyWebSocket::new(id, broadcaster.get_ref().clone());
    ws::start(ws, &req, stream)
}

#[get("/broadcast")]
// HTTP route to trigger a broadcast
async fn broadcast(
    req: HttpRequest,
    broadcaster: web::Data<Addr<Broadcaster>>,
    msg: web::Json<String>,
) -> Result<HttpResponse, Error> {
    broadcaster.do_send(BroadcastMessage {
        content: msg.into_inner(),
    });
    Ok(HttpResponse::Ok().body("Broadcast sent"))
}
