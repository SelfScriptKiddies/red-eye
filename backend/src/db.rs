use log::{debug, error};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Error, Pool, Postgres};
use crate::configuration::ApplicationConfig;

pub async fn establish_connection(config: &ApplicationConfig) -> Pool<Postgres> {
    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        &config.db_username,
        &config.db_password,
        &config.db_host,
        &config.db_port,
        &config.db_schema
    );

    debug!("postgresql: {:?}", database_url);
    PgPoolOptions::new()
        .max_connections(25)
        .connect(database_url.as_str())
        .await
        .unwrap_or_else(|error: Error| {
            error!("Can't connect to db: {:?}", error);
            std::process::exit(1)
        })
}