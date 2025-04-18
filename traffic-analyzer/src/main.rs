use tokio;

#[tokio::main]
async fn main() {
    traffic_analyzer::run().await;
}
