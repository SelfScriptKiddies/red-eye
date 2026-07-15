use log::{debug, error};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Error, Pool, Postgres};
use crate::configuration::ApplicationConfig;

pub async fn establish_connection(config: &ApplicationConfig) -> Pool<Postgres> {
    // Kill queries that exceed the configured budget; fall back to the historical 15s default.
    let statement_timeout = match config.health_kill_query_after_ms {
        0 => 15000,
        ms => ms,
    };
    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}?options=-c statement_timeout={}",
        &config.db_username,
        &config.db_password,
        &config.db_host,
        &config.db_port,
        &config.db_schema,
        statement_timeout
    );

    debug!("postgresql: {:?}", database_url);
    PgPoolOptions::new()
        .max_connections(50)
        .connect(database_url.as_str())
        .await
        .unwrap_or_else(|error: Error| {
            error!("Can't connect to db: {:?}", error);
            std::process::exit(1)
        })
}