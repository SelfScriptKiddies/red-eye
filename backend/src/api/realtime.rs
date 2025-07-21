use actix::{Actor, Addr};
use actix_web::{Error, HttpRequest, HttpResponse, get, rt, web};
use actix_ws::{AggregatedMessage, Session};
use futures_util::StreamExt as _;
use tokio::time::{Duration, sleep};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(echo);
}

//Websocket handler
#[get("/ws")]
async fn echo(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    //send 101 i guess
    let (res, mut session, _stream) = actix_ws::handle(&req, stream)?;

    // let mut stream = stream
    //     .aggregate_continuations()
    //     // aggregate continuation frames up to 1MiB
    //     .max_continuation_size(2_usize.pow(20));

    // spawn handler task
    rt::spawn(async move {
        loop {
            //dummy action. just send some silly string.
            // as there is no content-type header we can just send serde_json::to_string(T) instead
            session.text("update").await.unwrap();
            sleep(Duration::from_millis(1000)).await;
        }
        //code gathered from docs to Recieve message
        // receive messages from websocket
        // while let Some(msg) = stream.next().await {
        //     match msg {
        //         Ok(AggregatedMessage::Text(text)) => {
        //             // echo text message
        //             session.text(text).await.unwrap();
        //         }

        //         Ok(AggregatedMessage::Binary(bin)) => {
        //             // echo binary message
        //             session.binary(bin).await.unwrap();
        //         }

        //         Ok(AggregatedMessage::Ping(msg)) => {
        //             // respond to PING frame with PONG frame
        //             session.pong(&msg).await.unwrap();
        //         }

        //         _ => {}
        //     }
        // }
    });

    // respond immediately with response connected to WS session
    Ok(res)
}
