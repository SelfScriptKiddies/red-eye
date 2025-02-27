pub mod configuration;
use crate::configuration::init;
use log::info;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    println!("{:?}", config);
}
