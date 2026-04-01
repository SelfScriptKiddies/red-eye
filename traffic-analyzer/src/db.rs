use sqlx::{postgres::PgPoolOptions, PgPool, Error, Row};
use log::{info, error};
use crate::ethernet::{PacketInfo, Protocol, IpVersion};
use crate::configuration::ApplicationConfig;
use std::net::IpAddr;
use crate::process_packet::Direction;

pub struct DB {
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
            .max_connections(50)
            .connect(connection_string)
            .await?;
        
        info!("Database connection pool successfully initialized");
        Ok(Self { pgpool: pool })
    }

    pub async fn init(config: &ApplicationConfig) -> Result<Self, Error> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .connect(&format!("postgres://{}:{}@{}:{}/{}", config.db_username, config.db_password, config.db_host, config.db_port, config.db_schema))
            .await?;
        Ok(Self { pgpool: pool })
    }

    /// Get stream id from database
    pub async fn get_stream_id(&self, src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16) -> Option<usize> {
        let src_ip_addr: IpAddr = src_ip.parse().unwrap();
        let dst_ip_addr: IpAddr = dst_ip.parse().unwrap();

        // Try to find stream where source and destination match exactly
        let row = sqlx::query(
            "SELECT id FROM stream
             WHERE (src_ip = $1 AND src_port = $2 AND dst_ip = $3 AND dst_port = $4)
             AND active = true
             LIMIT 1"
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
        .bind(dst_port as i32)
        .fetch_optional(&self.pgpool)
        .await;

        match row {
            Ok(Some(row)) => Some(row.get::<i32, _>("id") as usize),
            Ok(None) => None,
            Err(e) => {
                error!("Error fetching stream id: {}", e);
                None
            }
        }
    }

    pub async fn create_stream(&self, src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16, protocol: &Protocol, ip_version: &IpVersion, service_id: usize) -> Result<usize, Error> {
        let protocol_str: &'static str = protocol.into();
        let ip_version_str: &'static str = ip_version.into();

        let src_ip_addr: IpAddr = src_ip.parse().unwrap();
        let dst_ip_addr: IpAddr = dst_ip.parse().unwrap();

        sqlx::query(
            "INSERT INTO stream (src_ip, src_port, dst_ip, dst_port, protocol, active, service_id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id"
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
        .bind(dst_port as i32)
        .bind(protocol_str)
        .bind(true)
        .bind(service_id as i32)
        .fetch_one(&self.pgpool)
        .await.map(|row| row.get::<i32, _>("id") as usize)
        
    }

    pub async fn insert_packet(&self, packet: &PacketInfo, stream_id: usize, direction: &Direction) -> Result<i32, Error> {
        let direction_str: &'static str = match direction {
            Direction::Inbound => "dest",
            Direction::Outbound => "orig"
        };
        
        sqlx::query(
            "INSERT INTO packets (timestamp, timestamp_microseconds, direction, stream_id, packet_data) VALUES ($1, $2, $3, $4, $5) RETURNING id"
        )
        .bind(packet.timestamp_seconds as i64)
        .bind(packet.timestamp_microseconds as i64)
        .bind(direction_str)
        .bind(stream_id as i32)
        .bind(&packet.payload)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i32, _>("id"))
    }

    pub async fn insert_trigger(&self, packet_id: i32, filter_id: usize, start_index: usize, end_index: usize) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO tag (packet_id, tag_id, start_index, end_index) VALUES ($1, $2, $3, $4)"
        )
        .bind(packet_id)
        .bind(filter_id as i32)
        .bind(start_index as i32)
        .bind(end_index as i32)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Batch insert triggers (single multi-row INSERT)
    pub async fn insert_triggers_batch(&self, rows: &[(i32, usize, usize, usize)]) -> Result<(), Error> {
        if rows.is_empty() { return Ok(()); }

        // Build multi-row INSERT: INSERT INTO tag (packet_id, tag_id, start_index, end_index) VALUES ($1,$2,$3,$4), ($5,$6,$7,$8), ...
        let mut query = String::from("INSERT INTO tag (packet_id, tag_id, start_index, end_index) VALUES ");
        let mut params: Vec<i32> = Vec::with_capacity(rows.len() * 4);

        for (i, (packet_id, filter_id, start, end)) in rows.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            let base = i * 4;
            query.push_str(&format!("(${}, ${}, ${}, ${})", base + 1, base + 2, base + 3, base + 4));
            params.push(*packet_id);
            params.push(*filter_id as i32);
            params.push(*start as i32);
            params.push(*end as i32);
        }

        let mut q = sqlx::query(&query);
        for p in &params {
            q = q.bind(p);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Update stream's L7 protocol (only if currently 'unknown')
    pub async fn update_stream_l7_protocol(&self, stream_id: usize, protocol: &str) -> Result<(), Error> {
        sqlx::query(
            "UPDATE stream SET l7_protocol = $1 WHERE id = $2 AND l7_protocol = 'unknown'"
        )
        .bind(protocol)
        .bind(stream_id as i32)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Get or create a deduplicated header set. Returns the header_set id.
    pub async fn get_or_create_header_set(
        &self,
        headers: &[(String, String)],
    ) -> Result<i32, Error> {
        use std::collections::BTreeMap;
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        // Build sorted JSONB and compute hash
        let map: BTreeMap<&str, &str> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let json = serde_json::json!(map);

        let mut hasher = DefaultHasher::new();
        json.to_string().hash(&mut hasher);
        let hash = hasher.finish() as i64;

        // Try to find existing
        let existing = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM header_set WHERE hash = $1"
        )
        .bind(hash)
        .fetch_optional(&self.pgpool)
        .await?;

        if let Some(id) = existing {
            return Ok(id);
        }

        // Insert new (ON CONFLICT for race conditions)
        sqlx::query_scalar::<_, i32>(
            "INSERT INTO header_set (hash, headers) VALUES ($1, $2)
             ON CONFLICT (hash) DO UPDATE SET hash = header_set.hash
             RETURNING id"
        )
        .bind(hash)
        .bind(json)
        .fetch_one(&self.pgpool)
        .await
    }

    /// Insert an HTTP message with a reference to a deduplicated header set.
    pub async fn insert_http_message(
        &self,
        stream_id: usize,
        packet_id: i32,
        is_request: bool,
        method: Option<&str>,
        path: Option<&str>,
        status_code: Option<u16>,
        http_version: Option<&str>,
        content_type: Option<&str>,
        content_length: Option<i32>,
        body_offset: i32,
        body_length: i32,
        header_set_id: Option<i32>,
        timestamp: i64,
    ) -> Result<i32, Error> {
        sqlx::query(
            "INSERT INTO http_message (stream_id, packet_id, is_request, method, path, status_code, http_version, content_type, content_length, body_offset, body_length, header_set_id, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             RETURNING id"
        )
        .bind(stream_id as i32)
        .bind(packet_id)
        .bind(is_request)
        .bind(method)
        .bind(path)
        .bind(status_code.map(|c| c as i16))
        .bind(http_version)
        .bind(content_type)
        .bind(content_length)
        .bind(body_offset)
        .bind(body_length)
        .bind(header_set_id)
        .bind(timestamp)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i32, _>("id"))
    }

    /// Insert a gRPC message
    pub async fn insert_grpc_message(
        &self,
        stream_id: usize,
        http_message_id: i32,
        service_name: Option<&str>,
        method_name: Option<&str>,
        is_request: bool,
        decoded_json: &str,
        timestamp: i64,
    ) -> Result<i32, Error> {
        sqlx::query(
            "INSERT INTO grpc_message (stream_id, http_message_id, service_name, method_name, is_request, decoded_json, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING id"
        )
        .bind(stream_id as i32)
        .bind(http_message_id)
        .bind(service_name)
        .bind(method_name)
        .bind(is_request)
        .bind(decoded_json)
        .bind(timestamp)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i32, _>("id"))
    }
}
