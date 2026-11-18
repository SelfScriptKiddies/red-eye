use crate::configuration::ApplicationConfig;
use crate::ethernet::{IpVersion, PacketInfo, Protocol};
use crate::process_packet::Direction;
use log::{error, info, warn};
use sqlx::{postgres::PgPoolOptions, Error, PgPool, Postgres, Row};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

/// Build the COPY FROM STDIN body for a packet batch in PostgreSQL's
/// BINARY copy format. Why binary and not text:
///
/// - bytea in text COPY must be hex (`\x...` with the backslash itself
///   COPY-escaped to `\\x`), which doubles the payload bytes on the wire —
///   measured on the scratch stand, a 50k x 700 B batch hex stream is
///   ~70 MB and the transfer, not the server, becomes the bottleneck.
///   Binary copy sends payload bytes raw.
/// - No per-byte escaping work client-side: rows are a few length-prefixed
///   memcpy's (i64 fields network order, direction as UTF-8 bytes).
///
/// Format (PG docs "Binary Format"): 11-byte signature, i32 flags, i32
/// header extension length, then per tuple an i16 field count and per field
/// an i32 byte length followed by the raw value, terminated by i16 -1.
fn encode_packets_copy_body_binary(
    ids: &[i64],
    packets: &[crate::batch_flusher::ProcessedPacket],
) -> Vec<u8> {
    fn push_i16be(out: &mut Vec<u8>, v: i16) {
        out.extend_from_slice(&v.to_be_bytes());
    }
    fn push_i32be(out: &mut Vec<u8>, v: i32) {
        out.extend_from_slice(&v.to_be_bytes());
    }
    /// One field: i32 length + raw bytes.
    fn push_field(out: &mut Vec<u8>, bytes: &[u8]) {
        push_i32be(out, bytes.len() as i32);
        out.extend_from_slice(bytes);
    }

    let mut body: Vec<u8> = Vec::with_capacity(
        packets.len() * (2 + 6 * 4 + 8 * 4 + 4)
            + packets.iter().map(|p| p.payload.len() + 4).sum::<usize>(),
    );
    body.extend_from_slice(b"PGCOPY\n\xFF\r\n\x00"); // signature
    push_i32be(&mut body, 0); // flags
    push_i32be(&mut body, 0); // header extension length

    for (id, pkt) in ids.iter().zip(packets.iter()) {
        let direction_bytes: &[u8] = match pkt.direction.as_str() {
            "inbound" => b"dest",
            _ => b"orig",
        };
        push_i16be(&mut body, 6); // 6 fields per tuple
        push_field(&mut body, &id.to_be_bytes());
        push_field(&mut body, &(pkt.timestamp_seconds as i64).to_be_bytes());
        push_field(
            &mut body,
            &(pkt.timestamp_microseconds as i64).to_be_bytes(),
        );
        push_field(&mut body, direction_bytes);
        push_field(&mut body, &(pkt.stream_id as i64).to_be_bytes());
        push_field(&mut body, &pkt.payload);
    }

    push_i16be(&mut body, -1); // end of data
    body
}

pub struct DB {
    pgpool: PgPool,
}

/// Outcome of one idempotent HTTP message batch. `message_ids` holds
/// the prefetched id for EVERY input row (positional); only
/// `inserted_count` of them were actually persisted.
#[derive(Debug, Default, Clone)]
pub struct HttpMessageBatchInsert {
    pub message_ids: Vec<i64>,
    pub inserted_count: usize,
    /// New messages whose User-Agent contributed to aggregates.
    pub ua_messages_contributed: u64,
    /// Aggregate bucket rows written (inserts + updates).
    pub ua_rows_written: u64,
    /// Contributions whose value exceeded the size bound.
    pub ua_oversized: u64,
    /// Contributions moved to the overflow row by the cardinality bound.
    pub ua_cardinality_overflow: u64,
}

impl Clone for DB {
    /// Cheap: PgPool is internally Arc — clones share one connection pool.
    fn clone(&self) -> Self {
        Self {
            pgpool: self.pgpool.clone(),
        }
    }
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
            .connect(&format!(
                "postgres://{}:{}@{}:{}/{}?options=-c statement_timeout=30000",
                config.db_username,
                config.db_password,
                config.db_host,
                config.db_port,
                config.db_schema
            ))
            .await?;
        Ok(Self { pgpool: pool })
    }

    /// Get stream id from database
    pub async fn get_stream_id(
        &self,
        src_ip: &str,
        src_port: u16,
        dst_ip: &str,
        dst_port: u16,
    ) -> Option<usize> {
        let src_ip_addr: IpAddr = src_ip.parse().unwrap();
        let dst_ip_addr: IpAddr = dst_ip.parse().unwrap();

        // Try to find stream where source and destination match exactly.
        // ORDER BY id DESC: with the SYN lifecycle a tuple can briefly have
        // several active rows (the deferred active=false flush lags the new
        // connection's SYN) — the newest row is the live connection.
        let row = sqlx::query(
            "SELECT id FROM stream
             WHERE (src_ip = $1 AND src_port = $2 AND dst_ip = $3 AND dst_port = $4)
             AND active = true
             ORDER BY id DESC
             LIMIT 1",
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
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

    pub async fn create_stream(
        &self,
        src_ip: &str,
        src_port: u16,
        dst_ip: &str,
        dst_port: u16,
        protocol: &Protocol,
        ip_version: &IpVersion,
        service_id: usize,
        first_packet_time: i64,
        agent_id: Option<&str>,
    ) -> Result<usize, Error> {
        let protocol_str: &'static str = protocol.into();
        let _ip_version_str: &'static str = ip_version.into();

        // Parsed addresses come from the ethernet parser, never raw input;
        // a parse failure here would mean a corrupted cache entry, so it
        // degrades into a stream-creation error instead of a panic.
        let src_ip_addr: IpAddr = src_ip
            .parse()
            .map_err(|e| std::io::Error::other(format!("invalid src_ip '{}': {}", src_ip, e)))?;
        let dst_ip_addr: IpAddr = dst_ip
            .parse()
            .map_err(|e| std::io::Error::other(format!("invalid dst_ip '{}': {}", dst_ip, e)))?;

        sqlx::query(
            "INSERT INTO stream (src_ip, src_port, dst_ip, dst_port, protocol, active, service_id, first_packet_time, agent_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id"
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
        .bind(dst_port as i32)
        .bind(protocol_str)
        .bind(true)
        .bind(service_id as i64)
        .bind(first_packet_time)
        .bind(agent_id)
        .fetch_one(&self.pgpool)
        .await.map(|row| row.get::<i64, _>("id") as usize)
    }

    pub async fn insert_packet(
        &self,
        packet: &PacketInfo,
        stream_id: usize,
        direction: &Direction,
    ) -> Result<i64, Error> {
        let direction_str: &'static str = match direction {
            Direction::Inbound => "dest",
            Direction::Outbound => "orig",
        };

        sqlx::query(
            "INSERT INTO packets (timestamp, timestamp_microseconds, direction, stream_id, packet_data) VALUES ($1, $2, $3, $4, $5) RETURNING id"
        )
        .bind(packet.timestamp_seconds as i64)
        .bind(packet.timestamp_microseconds as i64)
        .bind(direction_str)
        .bind(stream_id as i64)
        .bind(&packet.payload)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i64, _>("id"))
    }

    /// Insert a single tag (filter trigger) row.
    /// `timestamp` MUST be the packet's capture time (same value as the
    /// packets row): the backend JOINs `p.timestamp = t.timestamp`, so a
    /// DB-clock default silently detaches the tag from its packet.
    pub async fn insert_trigger(
        &self,
        packet_id: i64,
        filter_id: usize,
        start_index: usize,
        end_index: usize,
        timestamp: i64,
    ) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(packet_id)
        .bind(filter_id as i64)
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
    pub async fn insert_triggers_batch(
        &self,
        rows: &[(i64, usize, usize, usize, i64)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }

        // Build multi-row INSERT: INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ($1..$5), ($6..$10), ...
        let mut query = String::from(
            "INSERT INTO tag (packet_id, tag_id, start_index, end_index, timestamp) VALUES ",
        );
        const COLS: usize = 5;

        for (i, _) in rows.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * COLS;
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5
            ));
        }

        let mut q = sqlx::query(&query);
        for (packet_id, filter_id, start, end, timestamp) in rows {
            q = q
                .bind(*packet_id)
                .bind(*filter_id as i64)
                .bind(*start as i32)
                .bind(*end as i32)
                .bind(*timestamp);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Batch insert threshold alert/block events. remote_ip travels as
    /// text with an INET cast (no IpNetwork dependency here).
    pub async fn insert_filter_events_batch(
        &self,
        rows: &[crate::filter_events::FilterEventRow],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut query = String::from(
            "INSERT INTO filter_event (filter_id, agent_id, service_id, stream_id, remote_ip, \
             kind, window_secs, match_count) VALUES ",
        );
        const COLS: usize = 8;
        for i in 0..rows.len() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * COLS;
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}::INET, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8
            ));
        }
        let mut q = sqlx::query(&query);
        for row in rows {
            q = q
                .bind(row.filter_id)
                .bind(&row.agent_id)
                .bind(row.service_id)
                .bind(row.stream_id)
                .bind(row.remote_ip.clone())
                .bind(&row.kind)
                .bind(row.window_secs)
                .bind(row.match_count);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Persist a temporary active block. A repeated block for the same
    /// (filter, agent, ip) extends the expiry only forward: an analyzer
    /// restart never shortens, and nothing ever extends, beyond the
    /// newest legitimate block request.
    pub async fn insert_active_block(
        &self,
        filter_id: i64,
        agent_id: &str,
        remote_ip: &str,
        blocked_until_epoch_secs: i64,
    ) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO active_block (filter_id, agent_id, remote_ip, blocked_until)
             VALUES ($1, $2, $3::INET, to_timestamp($4))
             ON CONFLICT (filter_id, agent_id, remote_ip) DO UPDATE SET
                blocked_until = GREATEST(active_block.blocked_until, to_timestamp($4))",
        )
        .bind(filter_id)
        .bind(agent_id)
        .bind(remote_ip)
        .bind(blocked_until_epoch_secs)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Update stream's L7 protocol (upgrades: unknown→http1→grpc)
    pub async fn update_stream_l7_protocol(
        &self,
        stream_id: usize,
        protocol: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            "UPDATE stream SET l7_protocol = $1 WHERE id = $2 AND (l7_protocol = 'unknown' OR ($1 = 'grpc' AND l7_protocol = 'http1'))"
        )
        .bind(protocol)
        .bind(stream_id as i64)
        .execute(&self.pgpool)
        .await
        .map(|_| ())
    }

    /// Build the dedup key + JSONB payload for a header set.
    /// Pure: no DB round-trip on the packet hot path — the header_set id is
    /// resolved per flush batch by [`Self::insert_http_messages_batch`]
    /// (same pattern as gRPC body sets).
    pub fn hash_headers(headers: &[(String, String)]) -> (i64, serde_json::Value) {
        use std::collections::hash_map::DefaultHasher;
        use std::collections::BTreeMap;
        use std::hash::{Hash, Hasher};

        let map: BTreeMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let json = serde_json::json!(map);

        let mut hasher = DefaultHasher::new();
        json.to_string().hash(&mut hasher);
        (hasher.finish() as i64, json)
    }

    /// Insert a gRPC message with deduplicated body
    pub async fn insert_grpc_message(
        &self,
        stream_id: usize,
        http_message_id: i64,
        service_name: Option<&str>,
        method_name: Option<&str>,
        is_request: bool,
        decoded_json: &str,
        timestamp: i64,
    ) -> Result<i64, Error> {
        // Dedup body via hash (like header_set)
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        decoded_json.hash(&mut hasher);
        let body_hash = hasher.finish() as i64;

        // Upsert into grpc_body_set
        let body_set_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO grpc_body_set (hash, decoded_json) VALUES ($1, $2)
             ON CONFLICT (hash) DO UPDATE SET decoded_json = EXCLUDED.decoded_json
             RETURNING id",
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
        .bind(stream_id as i64)
        .bind(http_message_id)
        .bind(service_name)
        .bind(method_name)
        .bind(is_request)
        .bind(body_set_id)
        .bind(timestamp)
        .fetch_one(&self.pgpool)
        .await
        .map(|row| row.get::<i64, _>("id"))
    }

    /// Batch insert packets. Hot path uses COPY FROM STDIN (text format)
    /// with pre-fetched sequence ids; on COPY failure it degrades to a
    /// multi-row INSERT for the same batch (a lost COPY is a slow batch,
    /// never a lost batch). Returns the packet ids in insertion order.
    pub async fn insert_packets_batch(
        &self,
        packets: &[crate::batch_flusher::ProcessedPacket],
    ) -> Result<Vec<i64>, Error> {
        if packets.is_empty() {
            return Ok(vec![]);
        }

        let ids = self.prefetch_packet_ids(packets.len()).await?;

        match self.insert_packets_batch_copy(&ids, packets).await {
            Ok(()) => Ok(ids),
            Err(e) => {
                warn!("Packet batch COPY failed ({:?}) — falling back to idempotent multi-row INSERT for this batch", e);
                self.insert_packets_batch_insert(&ids, packets).await?;
                Ok(ids)
            }
        }
    }

    async fn prefetch_packet_ids(&self, n: usize) -> Result<Vec<i64>, Error> {
        let ids_csv: String = sqlx::query_scalar(
            "SELECT string_agg(nextval('packets_id_seq')::text, ',') FROM generate_series(1, $1)",
        )
        .bind(n as i64)
        .fetch_one(&self.pgpool)
        .await?;
        let ids: Vec<i64> = ids_csv
            .split(',')
            .map(|s| s.parse::<i64>())
            .collect::<Result<_, _>>()
            .map_err(|e| Error::Io(std::io::Error::other(format!("bad nextval output: {}", e))))?;
        if ids.len() != n {
            return Err(Error::Io(std::io::Error::other(format!(
                "sequence prefetch returned {} ids for {} packets",
                ids.len(),
                n
            ))));
        }
        Ok(ids)
    }

    /// Insert a packet batch via `COPY ... FROM STDIN` (binary format).
    ///
    /// COPY cannot RETURNING, so packet ids for the tag/stream_tag rows are
    /// pre-fetched by the caller (see `prefetch_packet_ids`) and written
    /// WITH the rows (id is the first COPY column).
    async fn insert_packets_batch_copy(
        &self,
        ids: &[i64],
        packets: &[crate::batch_flusher::ProcessedPacket],
    ) -> Result<(), Error> {
        // Build the binary COPY body.
        let body = encode_packets_copy_body_binary(ids, packets);

        // Stream it in on a dedicated connection.
        let mut conn = self.pgpool.acquire().await?;
        let mut copy = conn
            .copy_in_raw("COPY packets (id, timestamp, timestamp_microseconds, direction, stream_id, packet_data) FROM STDIN WITH (FORMAT binary)")
            .await?;
        if let Err(e) = copy.send(body.as_slice()).await {
            // Leave the connection in a clean state for the next pool user
            // (best effort; sqlx also recovers on drop).
            let _ = copy.abort("batch copy send failed").await;
            return Err(e);
        }
        let inserted = copy.finish().await?;
        if inserted != packets.len() as u64 {
            warn!(
                "COPY inserted {} rows, expected {} — rows are committed, keeping prefetched ids",
                inserted,
                packets.len()
            );
        }
        Ok(())
    }

    async fn insert_packets_batch_insert(
        &self,
        ids: &[i64],
        packets: &[crate::batch_flusher::ProcessedPacket],
    ) -> Result<(), Error> {
        for (chunk_ids, chunk_packets) in ids.chunks(10_000).zip(packets.chunks(10_000)) {
            let mut query = String::from(
                "INSERT INTO packets (id, timestamp, timestamp_microseconds, direction, stream_id, packet_data) VALUES "
            );
            let mut param_idx = 1u32;

            for (i, _) in chunk_packets.iter().enumerate() {
                if i > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!(
                    "(${}, ${}, ${}, ${}, ${}, ${})",
                    param_idx,
                    param_idx + 1,
                    param_idx + 2,
                    param_idx + 3,
                    param_idx + 4,
                    param_idx + 5
                ));
                param_idx += 6;
            }
            query.push_str(" ON CONFLICT (id, timestamp) DO NOTHING");

            let mut q = sqlx::query(&query);
            for (id, pkt) in chunk_ids.iter().zip(chunk_packets.iter()) {
                let direction_str: &str = match pkt.direction.as_str() {
                    "inbound" => "dest",
                    _ => "orig",
                };
                q = q
                    .bind(id)
                    .bind(pkt.timestamp_seconds as i64)
                    .bind(pkt.timestamp_microseconds as i64)
                    .bind(direction_str)
                    .bind(pkt.stream_id as i64)
                    .bind(&pkt.payload);
            }

            q.execute(&self.pgpool).await?;
        }
        Ok(())
    }

    /// Insert a batch of HTTP messages idempotently and update the
    /// User-Agent aggregate buckets for the newly inserted rows.
    ///
    /// Message ids are prefetched from the sequence (packet-id pattern)
    /// and written WITH the row; ON CONFLICT (id, timestamp) DO NOTHING
    /// RETURNING reports which rows are actually new, so a redelivered or
    /// retried batch returns no new rows and contributes no aggregates.
    ///
    /// The header_set upsert, the message insert and the aggregate upsert
    /// share ONE transaction: a crashed transaction persists neither the
    /// messages nor their aggregates, so the retry supplies both. With
    /// `with_ua_aggregates` false (degradation path after a failed
    /// aggregate attempt) the aggregates are skipped and ingestion still
    /// proceeds.
    pub async fn insert_http_messages_batch(
        &self,
        rows: &[(i64, &crate::batch_flusher::HttpMessageData)],
        with_ua_aggregates: bool,
    ) -> Result<HttpMessageBatchInsert, Error> {
        if rows.is_empty() {
            return Ok(HttpMessageBatchInsert::default());
        }

        let ids = self.prefetch_packet_ids(rows.len()).await?;
        let mut tx = self.pgpool.begin().await?;

        // Resolve header_set ids per batch (same pattern as gRPC body
        // sets). The hash is UNIQUE in header_set, so the upsert both
        // dedups and REVIVES sets deleted by cleanup_old_data's orphan
        // GC -- the old per-packet in-memory cache happily reused ids of
        // deleted rows, leaving http_messages whose headers rendered
        // empty in the UI.
        let mut distinct: HashMap<i64, &str> = HashMap::new();
        for (_, http) in rows {
            if let (Some(hash), Some(json)) = (http.header_hash, http.header_json.as_deref()) {
                distinct.entry(hash).or_insert(json);
            }
        }

        let mut id_by_hash: HashMap<i64, i64> = HashMap::new();
        if !distinct.is_empty() {
            let distinct: Vec<(i64, &str)> = distinct.into_iter().collect();
            let mut q1 = String::from("INSERT INTO header_set (hash, headers) VALUES ");
            for (i, _) in distinct.iter().enumerate() {
                if i > 0 {
                    q1.push_str(", ");
                }
                let base = i * 2;
                // header_json is carried as text -- cast to the column type.
                q1.push_str(&format!("(${}, ${}::jsonb)", base + 1, base + 2));
            }
            q1.push_str(
                " ON CONFLICT (hash) DO UPDATE SET headers = EXCLUDED.headers RETURNING hash, id",
            );
            let mut query1 = sqlx::query(&q1);
            for (h, j) in &distinct {
                query1 = query1.bind(*h).bind(*j);
            }
            for row in query1.fetch_all(&mut *tx).await? {
                id_by_hash.insert(row.get::<i64, _>("hash"), row.get::<i64, _>("id"));
            }
        }

        const COLS: usize = 14;
        let mut query = String::from(
            "INSERT INTO http_message (id, stream_id, packet_id, is_request, method, path, status_code, http_version, content_type, content_length, body_offset, body_length, header_set_id, timestamp) VALUES ",
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * COLS;
            query.push('(');
            for c in 0..COLS {
                if c > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!("${}", base + c + 1));
            }
            query.push(')');
        }
        // RETURNING lists ONLY the rows this statement actually inserted;
        // conflicting (redelivered) rows are skipped and add nothing.
        query.push_str(
            " ON CONFLICT (id, timestamp) DO NOTHING RETURNING id, stream_id, header_set_id, is_request, timestamp",
        );

        let mut q = sqlx::query_as::<_, (i64, i64, Option<i64>, bool, i64)>(&query);
        for (row_idx, (packet_id, http)) in rows.iter().enumerate() {
            let header_set_id = http.header_hash.and_then(|h| id_by_hash.get(&h).copied());
            q = q
                .bind(ids[row_idx])
                .bind(http.stream_id as i64)
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
                .bind(header_set_id)
                .bind(http.timestamp);
        }
        let inserted = q.fetch_all(&mut *tx).await?;
        let newly: HashSet<i64> = inserted.iter().map(|(id, _, _, _, _)| *id).collect();

        let mut outcome = HttpMessageBatchInsert {
            message_ids: ids.clone(),
            inserted_count: newly.len(),
            ..Default::default()
        };
        if with_ua_aggregates && !newly.is_empty() {
            match self.apply_ua_aggregates(&mut tx, rows, &ids, &newly).await {
                Ok((contributed, written, oversized, cardinality_overflow)) => {
                    outcome.ua_messages_contributed = contributed;
                    outcome.ua_rows_written = written;
                    outcome.ua_oversized = oversized;
                    outcome.ua_cardinality_overflow = cardinality_overflow;
                }
                Err(e) => {
                    // A failed statement aborts the whole transaction, so
                    // nothing is persisted; the caller retries the batch
                    // without aggregates (User-Agent stats are degradable,
                    // message storage is not).
                    return Err(e);
                }
            }
        }

        tx.commit().await?;
        Ok(outcome)
    }

    /// Group the batch's newly inserted request messages into User-Agent
    /// bucket increments and upsert them in the caller's transaction.
    /// Stream service/agent identity comes from the stream rows; scopes
    /// without identity are skipped. Returns (messages contributed, rows
    /// written, oversized values, messages moved to overflow by the
    /// cardinality bound).
    async fn apply_ua_aggregates(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        rows: &[(i64, &crate::batch_flusher::HttpMessageData)],
        ids: &[i64],
        newly: &HashSet<i64>,
    ) -> Result<(u64, u64, u64, u64), Error> {
        use crate::user_agent::{
            apply_cardinality_bound, group_contributions, BucketCardinality, UserAgentStat,
        };

        let is_new = |pos: usize| newly.contains(&ids[pos]);

        let identities: HashMap<i64, (i64, Option<String>)> =
            sqlx::query("SELECT id, service_id, agent_id FROM stream WHERE id = ANY($1)")
                .bind(
                    rows.iter()
                        .filter(|(_, http)| !matches!(http.user_agent, UserAgentStat::None))
                        .map(|(_, http)| http.stream_id as i64)
                        .collect::<Vec<i64>>(),
                )
                .fetch_all(&mut **tx)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        row.get::<i64, _>("id"),
                        (
                            row.get::<i64, _>("service_id"),
                            row.get::<Option<String>, _>("agent_id"),
                        ),
                    )
                })
                .collect();

        let mut oversized: u64 = 0;
        let contributions: Vec<(i64, String, i64, UserAgentStat)> = rows
            .iter()
            .enumerate()
            .filter(|(pos, _)| is_new(*pos))
            .filter_map(|(_pos, (_, http))| {
                let identity = identities.get(&(http.stream_id as i64))?;
                let agent_id = identity.1.clone()?;
                if matches!(http.user_agent, UserAgentStat::Overflow(_)) {
                    oversized += 1;
                }
                Some((
                    identity.0,
                    agent_id,
                    http.timestamp,
                    http.user_agent.clone(),
                ))
            })
            .collect();
        let contributed = contributions.len() as u64;
        if contributions.is_empty() {
            return Ok((0, 0, oversized, 0));
        }

        let mut groups = group_contributions(contributions);

        // Cardinality bound per (service, agent, bucket) scope touched with
        // ordinary values: stored values keep updating, everything new
        // beyond the remaining capacity collapses into the overflow row.
        let mut cardinality_overflow: u64 = 0;
        let mut scopes: Vec<(i64, String, i64)> = groups
            .keys()
            .filter(|(_, _, _, _, overflow)| !*overflow)
            .map(|(svc, agt, bkt, _, _)| (*svc, agt.clone(), *bkt))
            .collect();
        scopes.sort();
        scopes.dedup();
        for (service_id, agent_id, bucket) in &scopes {
            let batch_values: Vec<String> = groups
                .keys()
                .filter(|(svc, agt, bkt, _, overflow)| {
                    !*overflow && svc == service_id && agt == agent_id && bkt == bucket
                })
                .map(|(_, _, _, ua, _)| ua.clone())
                .collect();
            let (ordinary_total, known): (i64, Vec<String>) = sqlx::query_as(
                r#"
                SELECT count(*) AS ordinary_total,
                       COALESCE(array_agg(t.user_agent)
                                FILTER (WHERE t.user_agent = ANY($4::text[])),
                                '{}'::text[]) AS known
                FROM (
                    SELECT user_agent FROM http_user_agent_bucket
                    WHERE service_id = $1 AND agent_id = $2
                      AND bucket_start = $3 AND is_overflow = FALSE
                    LIMIT 1000
                ) t
                "#,
            )
            .bind(service_id)
            .bind(agent_id)
            .bind(bucket)
            .bind(&batch_values)
            .fetch_one(&mut **tx)
            .await?;
            let cap = BucketCardinality {
                ordinary_total,
                known: known.into_iter().collect(),
            };
            cardinality_overflow +=
                apply_cardinality_bound(&mut groups, *service_id, agent_id, *bucket, &cap);
        }

        // One atomic set-based upsert: count accumulates, first_seen
        // minimizes, last_seen maximizes.
        let mut service_ids: Vec<i64> = Vec::with_capacity(groups.len());
        let mut agent_ids: Vec<String> = Vec::with_capacity(groups.len());
        let mut buckets: Vec<i64> = Vec::with_capacity(groups.len());
        let mut values: Vec<String> = Vec::with_capacity(groups.len());
        let mut counts: Vec<i64> = Vec::with_capacity(groups.len());
        let mut firsts: Vec<i64> = Vec::with_capacity(groups.len());
        let mut lasts: Vec<i64> = Vec::with_capacity(groups.len());
        let mut overflows: Vec<bool> = Vec::with_capacity(groups.len());
        for ((svc, agt, bkt, ua, ovf), agg) in groups.iter() {
            service_ids.push(*svc);
            agent_ids.push(agt.clone());
            buckets.push(*bkt);
            values.push(ua.clone());
            counts.push(agg.message_count);
            firsts.push(agg.first_seen);
            lasts.push(agg.last_seen);
            overflows.push(*ovf);
        }
        sqlx::query(
            r#"
            INSERT INTO http_user_agent_bucket
                (service_id, agent_id, bucket_start, user_agent,
                 message_count, first_seen, last_seen, is_overflow)
            SELECT * FROM unnest($1::bigint[], $2::varchar[], $3::bigint[], $4::text[],
                                 $5::bigint[], $6::bigint[], $7::bigint[], $8::boolean[])
            ON CONFLICT (service_id, agent_id, bucket_start, is_overflow, user_agent)
            DO UPDATE SET
                message_count = http_user_agent_bucket.message_count + EXCLUDED.message_count,
                first_seen = LEAST(http_user_agent_bucket.first_seen, EXCLUDED.first_seen),
                last_seen = GREATEST(http_user_agent_bucket.last_seen, EXCLUDED.last_seen)
            "#,
        )
        .bind(&service_ids)
        .bind(&agent_ids)
        .bind(&buckets)
        .bind(&values)
        .bind(&counts)
        .bind(&firsts)
        .bind(&lasts)
        .bind(&overflows)
        .execute(&mut **tx)
        .await?;

        // Honesty marker for the UI: when aggregation started. DO NOTHING
        // keeps the very first write; the seed row stores an empty value.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        sqlx::query(
            "INSERT INTO app_settings (key, value) \
             VALUES ('http_user_agent_aggregation_start', $1::text) \
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(now.to_string())
        .execute(&mut **tx)
        .await?;

        let written = groups.len() as u64;
        Ok((contributed, written, oversized, cardinality_overflow))
    }

    /// Batch insert gRPC messages. `rows` is (http_message_id, grpc).
    /// Dedups body sets by hash (mirrors insert_grpc_message) with two multi-row queries.
    pub async fn insert_grpc_messages_batch(
        &self,
        rows: &[(i64, &crate::batch_flusher::GrpcMessageData)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

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
            if i > 0 {
                q1.push_str(", ");
            }
            let base = i * 2;
            q1.push_str(&format!("(${}, ${})", base + 1, base + 2));
        }
        q1.push_str(" ON CONFLICT (hash) DO UPDATE SET decoded_json = EXCLUDED.decoded_json RETURNING hash, id");
        let mut query1 = sqlx::query(&q1);
        for (h, j) in &distinct {
            query1 = query1.bind(*h).bind(*j);
        }
        let mut id_by_hash: HashMap<i64, i64> = HashMap::new();
        for row in query1.fetch_all(&self.pgpool).await? {
            id_by_hash.insert(row.get::<i64, _>("hash"), row.get::<i64, _>("id"));
        }

        // Multi-row insert grpc_message.
        const COLS: usize = 7;
        let mut q2 = String::from(
            "INSERT INTO grpc_message (stream_id, http_message_id, service_name, method_name, is_request, body_set_id, timestamp) VALUES "
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 {
                q2.push_str(", ");
            }
            let base = i * COLS;
            q2.push('(');
            for c in 0..COLS {
                if c > 0 {
                    q2.push_str(", ");
                }
                q2.push_str(&format!("${}", base + c + 1));
            }
            q2.push(')');
        }
        let mut query2 = sqlx::query(&q2);
        for ((http_message_id, grpc), h) in rows.iter().zip(hashes.iter()) {
            let body_set_id = id_by_hash.get(h).copied();
            query2 = query2
                .bind(grpc.stream_id as i64)
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
    pub async fn update_stream_l7_protocol_batch(
        &self,
        rows: &[(usize, String)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut query = String::from("UPDATE stream s SET l7_protocol = v.proto FROM (VALUES ");
        for (i, _) in rows.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * 2;
            query.push_str(&format!("(${}::bigint, ${}::varchar)", base + 1, base + 2));
        }
        query.push_str(") AS v(stream_id, proto) WHERE s.id = v.stream_id AND (s.l7_protocol = 'unknown' OR (v.proto = 'grpc' AND s.l7_protocol = 'http1'))");
        let mut q = sqlx::query(&query);
        for (sid, proto) in rows {
            q = q.bind(*sid as i64).bind(proto);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Batch update stream last_packet_time (max), increment packet_count
    /// and accumulate the payload-byte estimate (retention_bytes feeds
    /// segment rotation and star-time capacity measurement).
    /// `rows` is (stream_id, max_timestamp, packet_count_delta, bytes_delta).
    /// Single set-based UPDATE.
    pub async fn update_streams_stats_batch(
        &self,
        rows: &[(usize, i64, i64, i64)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut query = String::from(
            "UPDATE stream s SET last_packet_time = GREATEST(COALESCE(s.last_packet_time, 0), v.ts), packet_count = s.packet_count + v.cnt, retention_bytes = s.retention_bytes + v.bytes FROM (VALUES "
        );
        for (i, _) in rows.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * 4;
            query.push_str(&format!(
                "(${}::bigint, ${}::bigint, ${}::bigint, ${}::bigint)",
                base + 1,
                base + 2,
                base + 3,
                base + 4
            ));
        }
        query.push_str(") AS v(stream_id, ts, cnt, bytes) WHERE s.id = v.stream_id");
        let mut q = sqlx::query(&query);
        for (sid, ts, cnt, bytes) in rows {
            q = q.bind(*sid as i64).bind(*ts).bind(*cnt).bind(*bytes);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Batch close streams (TCP FIN/RST lifecycle): single set-based UPDATE,
    /// idempotent. Called by the batch flusher from the dirty set drained in
    /// `Cache::drain_closed_streams`, so the per-packet hot path never waits
    /// on a close UPDATE.
    pub async fn set_streams_inactive_batch(&self, stream_ids: &[usize]) -> Result<u64, Error> {
        if stream_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<i64> = stream_ids.iter().map(|&id| id as i64).collect();
        sqlx::query("UPDATE stream SET active = false WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&self.pgpool)
            .await
            .map(|r| r.rows_affected())
    }

    /// Batch upsert distinct (stream_id, tag_name) pairs into stream_tag.
    /// Maintains the per-stream tag list INCREMENTALLY so the streams list
    /// never recomputes the tag x packets join (the old materialized view).
    /// Dedup by PK + ON CONFLICT DO NOTHING keeps this cheap and idempotent.
    pub async fn insert_stream_tags_batch(&self, pairs: &[(usize, String)]) -> Result<(), Error> {
        if pairs.is_empty() {
            return Ok(());
        }

        let mut query = String::from("INSERT INTO stream_tag (stream_id, tag_name) VALUES ");
        for (i, _) in pairs.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            let base = i * 2;
            query.push_str(&format!("(${}::bigint, ${}::text)", base + 1, base + 2));
        }
        query.push_str(" ON CONFLICT (stream_id, tag_name) DO NOTHING");

        let mut q = sqlx::query(&query);
        for (stream_id, tag_name) in pairs {
            q = q.bind(*stream_id as i64).bind(tag_name);
        }
        q.execute(&self.pgpool).await.map(|_| ())
    }

    /// Load all proto files grouped by service_id.
    /// Returns: service_id → Vec<(filename, content)>
    pub async fn get_all_proto_files(&self) -> HashMap<i64, Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT service_id, filename, content FROM proto_file ORDER BY service_id, id",
        )
        .fetch_all(&self.pgpool)
        .await
        .unwrap_or_default();

        let mut result: HashMap<i64, Vec<(String, String)>> = HashMap::new();
        for row in rows {
            let svc_id: i64 = row.get("service_id");
            let filename: String = row.get("filename");
            let content: String = row.get("content");
            result.entry(svc_id).or_default().push((filename, content));
        }
        result
    }

    /// favorite_segment_bytes from app_settings; None when unset/invalid so
    /// the caller applies its default. Rotation keeps working with a stale
    /// value between polls.
    pub async fn favorite_segment_limit(&self) -> Result<Option<i64>, Error> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT value FROM app_settings WHERE key = 'favorite_segment_bytes'",
        )
        .fetch_optional(&self.pgpool)
        .await?;
        Ok(raw
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0))
    }

    /// Starred segments for the retention tracker: (id, retention_bytes,
    /// segment_index). The partial favorite index keeps this cheap even
    /// with the full stream table loaded.
    pub async fn starred_streams_snapshot(&self) -> Result<Vec<(i64, i64, i32)>, Error> {
        sqlx::query_as::<_, (i64, i64, i32)>(
            "SELECT id, retention_bytes, segment_index FROM stream WHERE favorite = true",
        )
        .fetch_all(&self.pgpool)
        .await
    }

    /// Segment rotation for an over-limit starred segment, atomically:
    /// close the starred segment for packet assignment and open an unstarred
    /// continuation with the same tuple, service, protocol and agent.
    /// Returns the new segment id, or None when the stream was unstarred
    /// concurrently (the old segment stays the assignment target).
    #[allow(clippy::too_many_arguments)]
    pub async fn rotate_star_segment(
        &self,
        old_stream_id: usize,
        src_ip: &str,
        src_port: u16,
        dst_ip: &str,
        dst_port: u16,
        protocol: &Protocol,
        service_id: usize,
        first_packet_time: i64,
        agent_id: Option<&str>,
    ) -> Result<Option<usize>, Error> {
        let protocol_str: &'static str = protocol.into();
        let src_ip_addr: IpAddr = src_ip
            .parse()
            .map_err(|e| std::io::Error::other(format!("invalid src_ip '{}': {}", src_ip, e)))?;
        let dst_ip_addr: IpAddr = dst_ip
            .parse()
            .map_err(|e| std::io::Error::other(format!("invalid dst_ip '{}': {}", dst_ip, e)))?;

        let mut tx = self.pgpool.begin().await?;
        // Guarded close: only a still-starred segment is finalized. The
        // archive worker finishes its saved copy; retention_state stays
        // 'archiving' until the drain completes.
        let closed = sqlx::query_scalar::<_, (i64, i32)>(
            "UPDATE stream SET active = false WHERE id = $1 AND favorite = true
             RETURNING id, segment_index",
        )
        .bind(old_stream_id as i64)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((old_id, segment_index)) = closed else {
            tx.commit().await?;
            return Ok(None);
        };

        let new_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO stream (src_ip, src_port, dst_ip, dst_port, protocol, active,
                                service_id, first_packet_time, agent_id,
                                continuation_of, segment_index)
            VALUES ($1, $2, $3, $4, $5, true, $6, $7, $8, $9, $10)
            RETURNING id
            "#,
        )
        .bind(src_ip_addr)
        .bind(src_port as i32)
        .bind(dst_ip_addr)
        .bind(dst_port as i32)
        .bind(protocol_str)
        .bind(service_id as i64)
        .bind(first_packet_time)
        .bind(agent_id)
        .bind(old_id)
        .bind(segment_index + 1)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(new_id as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pkt(
        payload: Vec<u8>,
        direction: &str,
        stream_id: usize,
    ) -> crate::batch_flusher::ProcessedPacket {
        crate::batch_flusher::ProcessedPacket {
            timestamp_seconds: 1_700_000_000,
            timestamp_microseconds: 123_456,
            direction: direction.to_string(),
            stream_id,
            payload,
            triggers: vec![],
            http_message: None,
            grpc_message: None,
            nats_event: None,
        }
    }

    #[test]
    fn binary_copy_body_header_and_row_shape() {
        let ids = [7i64];
        let packets = vec![pkt(vec![0xde, 0xad], "inbound", 42)];
        let body = encode_packets_copy_body_binary(&ids, &packets);

        // Signature + flags + ext len.
        assert_eq!(&body[..11], b"PGCOPY\n\xFF\r\n\x00");
        assert_eq!(&body[11..15], &0i32.to_be_bytes());
        assert_eq!(&body[15..19], &0i32.to_be_bytes());

        // Row: i16 field count 6, then per field i32 len + bytes.
        let mut p = 19usize;
        let next_i16 = |p: &mut usize| -> i16 {
            let v = i16::from_be_bytes([body[*p], body[*p + 1]]);
            *p += 2;
            v
        };
        let next_field = |p: &mut usize| -> Vec<u8> {
            let len = i32::from_be_bytes(body[*p..*p + 4].try_into().unwrap()) as usize;
            *p += 4;
            let v = body[*p..*p + len].to_vec();
            *p += len;
            v
        };
        assert_eq!(next_i16(&mut p), 6);
        assert_eq!(next_field(&mut p), 7i64.to_be_bytes().to_vec()); // id
        assert_eq!(next_field(&mut p), 1_700_000_000i64.to_be_bytes().to_vec()); // timestamp
        assert_eq!(next_field(&mut p), 123_456i64.to_be_bytes().to_vec()); // timestamp_microseconds
        assert_eq!(next_field(&mut p), b"dest".to_vec()); // direction
        assert_eq!(next_field(&mut p), 42i64.to_be_bytes().to_vec()); // stream_id
        assert_eq!(next_field(&mut p), vec![0xde, 0xad]); // packet_data, raw
        assert_eq!(next_i16(&mut p), -1, "trailer");
        assert_eq!(p, body.len(), "no trailing bytes");
    }

    #[test]
    fn binary_copy_body_empty_payload_is_zero_length_field() {
        let body = encode_packets_copy_body_binary(&[9i64], &[pkt(vec![], "outbound", 1)]);
        // A zero-length field is length 0 and no bytes — the row must still
        // parse server-side (bytea '').
        assert!(body.windows(4).any(|w| w == 0i32.to_be_bytes()));
    }

    async fn test_db() -> Option<DB> {
        let url = std::env::var("DATABASE_TEST_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .expect("scratch PG reachable");
        Some(DB::new(pool))
    }

    /// Live COPY round-trip: every byte value (incl. \n \t \ \ .), then
    /// SELECT back and byte-compare payloads; ids must match the prefetched
    /// ones. Ignored by default — needs a scratch PG with the Red Eye schema:
    /// `DATABASE_TEST_URL=postgres://scratch:scratch@127.0.0.1:5599/scratch \
    ///  cargo test -p traffic-analyzer copy_roundtrip -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires scratch PG (DATABASE_TEST_URL, Red Eye schema applied)"]
    async fn copy_roundtrip_all_bytes_live() {
        let Some(db) = test_db().await else {
            eprintln!("DATABASE_TEST_URL not set, skipping");
            return;
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // 257 rows: one per byte value plus a multi-KB pseudo-random payload.
        let mut packets = Vec::new();
        for b in 0u8..=255 {
            packets.push(pkt(
                vec![b, b.wrapping_add(1), 0x0a, 0x09, 0x5c, 0x2e],
                if b % 2 == 0 { "inbound" } else { "outbound" },
                1,
            ));
        }
        let big: Vec<u8> = (0..5000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        packets.push(pkt(big, "inbound", 1));
        for p in &mut packets {
            p.timestamp_seconds = now;
        }

        let ids = db
            .insert_packets_batch(&packets)
            .await
            .expect("batch insert (COPY) ok");
        assert_eq!(ids.len(), packets.len(), "one id per row");
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            ids.len(),
            "ids unique"
        );

        let rows = sqlx::query("SELECT id, packet_data FROM packets WHERE stream_id = 1 AND timestamp = $1 ORDER BY id")
            .bind(now as i64)
            .fetch_all(db.pool()).await.unwrap();
        assert_eq!(rows.len(), packets.len(), "all rows landed");
        for (row, (id, p)) in rows.iter().zip(ids.iter().zip(packets.iter())) {
            assert_eq!(row.get::<i64, _>("id"), *id);
            let data: Vec<u8> = row.get("packet_data");
            assert_eq!(data, p.payload, "payload byte-identical for id {}", id);
        }
    }

    /// Live benchmark: multi-row INSERT vs COPY at 5k and 50k rows.
    /// Reports wall time per strategy. Payloads ~700 B of pseudo-random bytes
    /// (typical game mix of small and full-MTU frames).
    #[tokio::test]
    #[ignore = "requires scratch PG (DATABASE_TEST_URL); benchmark, run with --nocapture"]
    async fn bench_insert_vs_copy_live() {
        let Some(db) = test_db().await else {
            eprintln!("DATABASE_TEST_URL not set, skipping");
            return;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        sqlx::query("TRUNCATE packets")
            .execute(db.pool())
            .await
            .unwrap();

        let make = |n: usize| -> Vec<crate::batch_flusher::ProcessedPacket> {
            (0..n)
                .map(|i| {
                    let mut p = pkt(
                        (0..700usize)
                            .map(|j| ((i * 31 + j * 17) % 256) as u8)
                            .collect(),
                        if i % 3 == 0 { "inbound" } else { "outbound" },
                        1 + i % 50,
                    );
                    p.timestamp_seconds = now;
                    p
                })
                .collect()
        };

        for &n in &[500usize, 1_000, 2_000, 5_000, 50_000] {
            let packets = make(n);

            // COPY is not bound by the 65535-parameter limit, but multi-row
            // INSERT is (6 params/row -> max ~10k rows per statement): the
            // idempotent INSERT chunks internally, same as a real flusher.
            let t0 = std::time::Instant::now();
            let ids_ins = db.prefetch_packet_ids(n).await.expect("prefetch ok");
            db.insert_packets_batch_insert(&ids_ins, &packets)
                .await
                .expect("INSERT ok");
            let insert_ms = t0.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(ids_ins.len(), n);

            sqlx::query("TRUNCATE packets")
                .execute(db.pool())
                .await
                .unwrap();

            // Phase timings for COPY (what the flusher actually pays).
            let t0 = std::time::Instant::now();
            let ids_csv: String = sqlx::query_scalar(
                "SELECT string_agg(nextval('packets_id_seq')::text, ',') FROM generate_series(1, $1)")
                .bind(n as i64)
                .fetch_one(db.pool()).await.unwrap();
            let nextval_ms = t0.elapsed().as_secs_f64() * 1000.0;

            let t0 = std::time::Instant::now();
            let ids: Vec<i64> = ids_csv.split(',').map(|s| s.parse().unwrap()).collect();
            let body = encode_packets_copy_body_binary(&ids, &packets);
            let build_ms = t0.elapsed().as_secs_f64() * 1000.0;

            let t0 = std::time::Instant::now();
            let mut conn = db.pool().acquire().await.unwrap();
            let mut copy = conn
                .copy_in_raw("COPY packets (id, timestamp, timestamp_microseconds, direction, stream_id, packet_data) FROM STDIN WITH (FORMAT binary)")
                .await.unwrap();
            copy.send(body.as_slice()).await.unwrap();
            let inserted = copy.finish().await.unwrap();
            let stream_ms = t0.elapsed().as_secs_f64() * 1000.0;
            drop(conn);
            assert_eq!(inserted as usize, n);

            // The phase-instrumented COPY above already landed n rows —
            // clear before measuring the full wrapper path.
            sqlx::query("TRUNCATE packets")
                .execute(db.pool())
                .await
                .unwrap();

            let t0 = std::time::Instant::now();
            db.insert_packets_batch_copy(&ids, &packets)
                .await
                .expect("COPY ok");
            let copy_ms = t0.elapsed().as_secs_f64() * 1000.0;

            let count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM packets")
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(count as usize, n, "COPY landed all rows");

            eprintln!("BENCH rows={:>6}  multi-row INSERT (10k chunks): {:8.1} ms ({:6.0} rows/s)   COPY: {:8.1} ms ({:6.0} rows/s)  speedup {:.2}x   [COPY phases: nextval {:5.1} ms, build {:5.1} ms, stream {:5.1} ms]",
                n, insert_ms, n as f64 / (insert_ms / 1000.0), copy_ms, n as f64 / (copy_ms / 1000.0),
                insert_ms / copy_ms, nextval_ms, build_ms, stream_ms);

            sqlx::query("TRUNCATE packets")
                .execute(db.pool())
                .await
                .unwrap();
        }
    }

    /// Live: sequence-prefetch id assignment under concurrency — two tasks
    /// each prefetch interleaved nextval batches; all ids must be unique.
    /// This is what multiple flushers do at runtime.
    #[tokio::test]
    #[ignore = "requires scratch PG (DATABASE_TEST_URL)"]
    async fn nextval_batches_unique_under_concurrency_live() {
        let Some(db) = test_db().await else {
            eprintln!("DATABASE_TEST_URL not set, skipping");
            return;
        };
        let db = std::sync::Arc::new(db);

        let mut handles = Vec::new();
        for t in 0..2 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let mut ids = Vec::with_capacity(20 * 500);
                for i in 0..20 {
                    // Small stagger so the two tasks' nextval calls interleave.
                    if i % 2 == t {
                        tokio::time::sleep(std::time::Duration::from_micros(200)).await;
                    }
                    let batch: Vec<i64> = sqlx::query_scalar(
                        "SELECT nextval('packets_id_seq') FROM generate_series(1, $1)",
                    )
                    .bind(500i64)
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                    ids.extend(batch);
                }
                ids
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.await.unwrap());
        }
        let uniq: std::collections::HashSet<i64> = all.iter().copied().collect();
        assert_eq!(all.len(), 2 * 20 * 500);
        assert_eq!(
            uniq.len(),
            all.len(),
            "no id collisions across interleaved prefetches"
        );
    }
}
