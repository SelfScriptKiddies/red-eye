use config_manager::ConfigInit;
use crate::configuration::ApplicationConfig;

mod api;
mod configuration;



#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dbg!(ApplicationConfig::parse().unwrap());
    env_logger::init();
    Ok(())
}