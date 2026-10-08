use std::collections::HashMap;
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

    pub fn pool(&self) -> &PgPool {
        &self.pgpool
    }

    /// Initialize database connection pool
    pub async fn connect(connection_string: &str) -> Result<Self, Error> {
        info!("Initializing database connection pool");

        let url = if connection_string.contains('?') {
            format!("{}&options=-c statement_timeout=30000", connection_string)
        } else {
            format!("{}?options=-c statement_timeout=30000", connection_string)
        };

        let pool = PgPoolOptions::new()
            .max_connections(50)
            .connect(&url)
            .await?;

        info!("Database connection pool successfully initialized");
        Ok(Self { pgpool: pool })
    }

    pub async fn init(config: &ApplicationConfig) -> Result<Self, Error> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .connect(&format!("postgres://{}:{}@{}:{}/{}?options=-c statement_timeout=30000", config.db_username, config.db_password, config.db_host, config.db_port, config.db_schema))
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

    pub async fn create_stream(&self, src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16, protocol: &Protocol, ip_version: &IpVersion, service_id: usize, first_packet_time: i64) -> Result<usize, Error> {
        let protocol_str: &'static str = protocol.into();
        let ip_version_str: &'static str = ip_version.into();

        let src_ip_addr: IpAddr = src_ip.parse().unwrap();
        let dst_ip_addr: IpAddr = dst_ip.parse().unwrap();

        sqlx::query(
            "INSERT INTO stream (src_ip, src_port, dst_ip, dst_port, protocol, active, service_id, first_packet_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id"
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
        .bind(dst_port as i32)
        .bind(protocol_str)
        .bind(true)
        .bind(service_id as i32)
        .bind(first_packet_time)
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

    /// Insert a single tag (filter trigger) row.
    /// `timestamp` MUST be the packet's capture time (same value as the
    /// packets row): the backend JOINs `p.timestamp = t.timestamp`, so a
    /// DB-clock default silently detaches the tag from its packet.
    pub async fn insert_trigger(&self, packet_id: i32, filter_id: usize, start_index: usize, end_index: usize, timestamp: i64) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(packet_id)
        .bind(filter_id as i32)
        .bind(start_index as i32)
        .bind(end_index as i32)
        .bind(timestamp)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Batch insert triggers (single multi-row INSERT).
    /// Rows are (packet_id, filter_id, start_index, end_index, packet_timestamp):
    /// the timestamp is each source packet's capture time, NOT NOW() — the
    /// backend joins tag rows to packets on `p.timestamp = t.timestamp`.
    pub async fn insert_triggers_batch(&self, rows: &[(i32, usize, usize, usize, i64)]) -> Result<(), Error> {
        if rows.is_empty() { return Ok(()); }

        // Build multi-row INSERT: INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ($1..$5), ($6..$10), ...
        let mut query = String::from("INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ");
        const COLS: usize = 5;

        for (i, _) in rows.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            let base = i * COLS;
            query.push_str(&format!("(${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5));
        }

        let mut q = sqlx::query(&query);
        for (packet_id, filter_id, start, end, timestamp) in rows {
            q = q.bind(*packet_id)
                 .bind(*filter_id as i32)
                 .bind(*start as i32)
                 .bind(*end as i32)
                 .bind(*timestamp);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Update stream's L7 protocol (upgrades: unknown→http1→grpc)
    pub async fn update_stream_l7_protocol(&self, stream_id: usize, protocol: &str) -> Result<(), Error> {
        sqlx::query(
            "UPDATE stream SET l7_protocol = $1 WHERE id = $2 AND (l7_protocol = 'unknown' OR ($1 = 'grpc' AND l7_protocol = 'http1'))"
        )
        .bind(protocol)
        .bind(stream_id as i32)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Get or create a deduplicated header set. Returns the header_set id.
    /// Uses in-memory cache to avoid DB round-trips for repeated header sets.
    pub async fn get_or_create_header_set(
        &self,
        headers: &[(String, String)],
        cache: &dashmap::DashMap<i64, i32>,
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

        // Check in-memory cache first
        if let Some(id) = cache.get(&hash) {
            return Ok(*id);
        }

        // Try to find existing in DB
        let existing = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM header_set WHERE hash = $1"
        )
        .bind(hash)
        .fetch_optional(&self.pgpool)
        .await?;

        if let Some(id) = existing {
            cache.insert(hash, id);
            return Ok(id);
        }

        // Insert new (ON CONFLICT for race conditions)
        let id = sqlx::query_scalar::<_, i32>(
            "INSERT INTO header_set (hash, headers) VALUES ($1, $2)
             ON CONFLICT (hash) DO UPDATE SET hash = header_set.hash
             RETURNING id"
        )
        .bind(hash)
        .bind(json)
        .fetch_one(&self.pgpool)
        .await?;

        cache.insert(hash, id);
        Ok(id)
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

    /// Insert a gRPC message with deduplicated body
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
        // Dedup body via hash (like header_set)
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        decoded_json.hash(&mut hasher);
        let body_hash = hasher.finish() as i64;

        // Upsert into grpc_body_set
        let body_set_id = sqlx::query_scalar::<_, i32>(
            "INSERT INTO grpc_body_set (hash, decoded_json) VALUES ($1, $2)
             ON CONFLICT (hash) DO UPDATE SET decoded_json = EXCLUDED.decoded_json
             RETURNING id"
        )
        .bind(body_hash)
        .bind(decoded_json)
        .fetch_one(&self.pgpool)
        .await?;

        sqlx::query(
            "INSERT INTO grpc_message (stream_id, http_message_id, service_name, method_name, is_request, body_set_id, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING id"
        )
        .bind(stream_id as i32)
        .bind(http_message_id)
        .bind(service_name)
        .bind(method_name)
        .bind(is_request)
        .bind(body_set_id)
        .bind(timestamp)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i32, _>("id"))
    }

    /// Batch insert packets with a single multi-row INSERT.
    /// Returns the list of generated packet IDs in insertion order.
    pub async fn insert_packets_batch(&self, packets: &[crate::batch_flusher::ProcessedPacket]) -> Result<Vec<i32>, Error> {
        if packets.is_empty() { return Ok(vec![]); }

        let mut query = String::from(
            "INSERT INTO packets (timestamp, timestamp_microseconds, direction, stream_id, packet_data) VALUES "
        );
        let mut param_idx = 1u32;

        for (i, _) in packets.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            query.push_str(&format!("(${}, ${}, ${}, ${}, ${})",
                param_idx, param_idx + 1, param_idx + 2, param_idx + 3, param_idx + 4));
            param_idx += 5;
        }
        query.push_str(" RETURNING id");

        let mut q = sqlx::query_scalar::<_, i32>(&query);
        for pkt in packets {
            let direction_str: &str = match pkt.direction.as_str() {
                "inbound" => "dest",
                _ => "orig",
            };
            q = q.bind(pkt.timestamp_seconds as i64)
                 .bind(pkt.timestamp_microseconds as i64)
                 .bind(direction_str)
                 .bind(pkt.stream_id as i32)
                 .bind(&pkt.payload);
        }

        q.fetch_all(&self.pgpool).await
    }

    /// Batch insert HTTP messages with a single multi-row INSERT.
    /// `rows` is (packet_id, http). Returns generated ids in insertion order.
    pub async fn insert_http_messages_batch(
        &self,
        rows: &[(i32, &crate::batch_flusher::HttpMessageData)],
    ) -> Result<Vec<i32>, Error> {
        if rows.is_empty() { return Ok(vec![]); }

        const COLS: usize = 13;
        let mut query = String::from(
            "INSERT INTO http_message (stream_id, packet_id, is_request, method, path, status_code, http_version, content_type, content_length, body_offset, body_length, header_set_id, timestamp) VALUES "
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            let base = i * COLS;
            query.push('(');
            for c in 0..COLS {
                if c > 0 { query.push_str(", "); }
                query.push_str(&format!("${}", base + c + 1));
            }
            query.push(')');
        }
        query.push_str(" RETURNING id");

        let mut q = sqlx::query_scalar::<_, i32>(&query);
        for (packet_id, http) in rows {
            q = q.bind(http.stream_id as i32)
                 .bind(*packet_id)
                 .bind(http.is_request)
                 .bind(http.method.as_deref())
                 .bind(http.path.as_deref())
                 .bind(http.status_code.map(|c| c as i16))
                 .bind(http.http_version.as_deref())
                 .bind(http.content_type.as_deref())
                 .bind(http.content_length)
                 .bind(http.body_offset)
                 .bind(http.body_length)
                 .bind(http.header_set_id)
                 .bind(http.timestamp);
        }
        q.fetch_all(&self.pgpool).await
    }

    /// Batch insert gRPC messages. `rows` is (http_message_id, grpc).
    /// Dedups body sets by hash (mirrors insert_grpc_message) with two multi-row queries.
    pub async fn insert_grpc_messages_batch(
        &self,
        rows: &[(i32, &crate::batch_flusher::GrpcMessageData)],
    ) -> Result<(), Error> {
        if rows.is_empty() { return Ok(()); }
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        // Compute per-row body hash and dedupe distinct body sets.
        let mut hashes: Vec<i64> = Vec::with_capacity(rows.len());
        let mut distinct: HashMap<i64, &str> = HashMap::new();
        for (_, grpc) in rows {
            let mut hasher = DefaultHasher::new();
            grpc.decoded_json.hash(&mut hasher);
            let h = hasher.finish() as i64;
            hashes.push(h);
            distinct.entry(h).or_insert(grpc.decoded_json.as_str());
        }

        // Multi-row upsert distinct body sets -> map hash -> id.
        let distinct: Vec<(i64, &str)> = distinct.into_iter().collect();
        let mut q1 = String::from("INSERT INTO grpc_body_set (hash, decoded_json) VALUES ");
        for (i, _) in distinct.iter().enumerate() {
            if i > 0 { q1.push_str(", "); }
            let base = i * 2;
            q1.push_str(&format!("(${}, ${})", base + 1, base + 2));
        }
        q1.push_str(" ON CONFLICT (hash) DO UPDATE SET decoded_json = EXCLUDED.decoded_json RETURNING hash, id");
        let mut query1 = sqlx::query(&q1);
        for (h, j) in &distinct {
            query1 = query1.bind(*h).bind(*j);
        }
        let mut id_by_hash: HashMap<i64, i32> = HashMap::new();
        for row in query1.fetch_all(&self.pgpool).await? {
            id_by_hash.insert(row.get::<i64, _>("hash"), row.get::<i32, _>("id"));
        }

        // Multi-row insert grpc_message.
        const COLS: usize = 7;
        let mut q2 = String::from(
            "INSERT INTO grpc_message (stream_id, http_message_id, service_name, method_name, is_request, body_set_id, timestamp) VALUES "
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 { q2.push_str(", "); }
            let base = i * COLS;
            q2.push('(');
            for c in 0..COLS {
                if c > 0 { q2.push_str(", "); }
                q2.push_str(&format!("${}", base + c + 1));
            }
            q2.push(')');
        }
        let mut query2 = sqlx::query(&q2);
        for ((http_message_id, grpc), h) in rows.iter().zip(hashes.iter()) {
            let body_set_id = id_by_hash.get(h).copied();
            query2 = query2
                .bind(grpc.stream_id as i32)
                .bind(*http_message_id)
                .bind(grpc.service_name.as_deref())
                .bind(grpc.method_name.as_deref())
                .bind(grpc.is_request)
                .bind(body_set_id)
                .bind(grpc.timestamp);
        }
        query2.execute(&self.pgpool).await.map(|_| ())
    }

    /// Batch update stream l7_protocol from deduped (stream_id, protocol) pairs.
    /// Keeps the upgrade semantics of update_stream_l7_protocol (unknown->http1->grpc).
    pub async fn update_stream_l7_protocol_batch(&self, rows: &[(usize, String)]) -> Result<(), Error> {
        if rows.is_empty() { return Ok(()); }
        let mut query = String::from("UPDATE stream s SET l7_protocol = v.proto FROM (VALUES ");
        for (i, _) in rows.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            let base = i * 2;
            query.push_str(&format!("(${}::int, ${}::varchar)", base + 1, base + 2));
        }
        query.push_str(") AS v(stream_id, proto) WHERE s.id = v.stream_id AND (s.l7_protocol = 'unknown' OR (v.proto = 'grpc' AND s.l7_protocol = 'http1'))");
        let mut q = sqlx::query(&query);
        for (sid, proto) in rows {
            q = q.bind(*sid as i32).bind(proto);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Batch update stream last_packet_time (max) and increment packet_count.
    /// `rows` is (stream_id, max_timestamp, packet_count_delta). Single set-based UPDATE.
    pub async fn update_streams_stats_batch(&self, rows: &[(usize, i64, i64)]) -> Result<(), Error> {
        if rows.is_empty() { return Ok(()); }
        let mut query = String::from(
            "UPDATE stream s SET last_packet_time = GREATEST(COALESCE(s.last_packet_time, 0), v.ts), packet_count = s.packet_count + v.cnt FROM (VALUES "
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 { query.push_str(", "); }
            let base = i * 3;
            query.push_str(&format!("(${}::int, ${}::bigint, ${}::int)", base + 1, base + 2, base + 3));
        }
        query.push_str(") AS v(stream_id, ts, cnt) WHERE s.id = v.stream_id");
        let mut q = sqlx::query(&query);
        for (sid, ts, cnt) in rows {
            q = q.bind(*sid as i32).bind(*ts).bind(*cnt as i32);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Send pg_notify for materialized view / stream refresh.
    pub async fn notify_refresh_streams(&self) -> Result<(), Error> {
        sqlx::query("SELECT pg_notify('refresh_streams', '')")
            .execute(&self.pgpool)
            .await
            .map(|_| ())
    }

    /// Load all proto files grouped by service_id.
    /// Returns: service_id → Vec<(filename, content)>
    pub async fn get_all_proto_files(&self) -> HashMap<i32, Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT service_id, filename, content FROM proto_file ORDER BY service_id, id"
        )
        .fetch_all(&self.pgpool)
        .await
        .unwrap_or_default();

        let mut result: HashMap<i32, Vec<(String, String)>> = HashMap::new();
        for row in rows {
            let svc_id: i32 = row.get("service_id");
            let filename: String = row.get("filename");
            let content: String = row.get("content");
            result.entry(svc_id).or_default().push((filename, content));
        }
        result
    }
}
