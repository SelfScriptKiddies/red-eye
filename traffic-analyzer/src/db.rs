use sqlx::{postgres::PgPoolOptions, PgPool, Error, Row};
use log::{info, error};
use crate::filters::filterDTO::FilterTrigger;

struct DB {
    pgpool: PgPool,
}

impl DB {
    pub fn new(pgpool: PgPool) -> Self {
        Self { pgpool }
    }

    /// Initialize database connection pool
    pub async fn connect(connection_string: &str) -> Result<Self, Error> {
        info!("Initializing database connection pool");
        
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(connection_string)
            .await?;
        
        info!("Database connection pool successfully initialized");
        Ok(Self { pgpool: pool })
    }

    /// Get stream id from database
    pub async fn get_stream_id(&self, src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16) -> Option<usize> {
        // Try to find stream where source and destination match exactly
        let row = sqlx::query(
            "SELECT id FROM streams 
             WHERE ((src_ip = $1 AND src_port = $2 AND dst_ip = $3 AND dst_port = $4)
             OR (src_ip = $3 AND src_port = $4 AND dst_ip = $1 AND dst_port = $2))
             AND active = true
             LIMIT 1"
        )
        .bind(src_ip)
        .bind(src_port as i32)
        .bind(dst_ip)
        .bind(dst_port as i32)
        .fetch_optional(&self.pgpool)
        .await;

        match row {
            Ok(Some(row)) => Some(row.get::<i64, _>("id") as usize),
            Ok(None) => None,
            Err(e) => {
                error!("Error fetching stream id: {}", e);
                None
            }
        }
    }
    
}
