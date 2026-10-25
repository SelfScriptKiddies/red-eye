//! Live check of the agent's Docker discovery path against the real Docker
//! daemon on this machine, plus a real NATS publish/subscribe round-trip of
//! the reply DTO.
//!
//! Usage:
//!   cargo run -p red-eye-agent --example list_containers [-- nats://host:port]
//!
//! Without a NATS argument it only lists containers (prints the exact JSON
//! the agent would publish). With a NATS URL it also subscribes to
//! `agent_docker.example-agent`, publishes the real payload there and prints
//! what came back — proving the wire format survives the broker.

use futures::StreamExt;
use red_eye_agent::docker::{self, DockerContainersStatus};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() {
    let nats_url = std::env::args().nth(1);

    let containers = match docker::list_running_containers().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("docker listing failed: {}", e);
            std::process::exit(1);
        }
    };

    let status = DockerContainersStatus {
        agent_id: "example-agent".to_string(),
        timestamp_ms: now_ms(),
        error: None,
        containers,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&status).expect("serialize status")
    );

    if let Some(url) = nats_url {
        let client = async_nats::connect(&url).await.expect("connect nats");
        let topic = "agent_docker.example-agent".to_string();
        let mut sub = client.subscribe(topic.clone()).await.expect("subscribe");

        let payload = serde_json::to_vec(&status).expect("serialize");
        client
            .publish(topic.clone(), payload.into())
            .await
            .expect("publish");
        client.flush().await.expect("flush");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
            .await
            .expect("receive within 5s")
            .expect("message");
        println!("\n--- round-trip through NATS ({}) ---", url);
        println!("{}", String::from_utf8_lossy(&msg.payload));
    }
}
