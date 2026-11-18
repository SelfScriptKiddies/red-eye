use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

// Incremental stream transcript builder.
//
// One entry per content-bearing packet, both directions in capture order
// (timestamp, microseconds, id), through the live + saved union layer so
// starred streams keep analyzing after their partitions are dropped.
// Selection is deterministic for the same stream revision: fixed limits,
// fixed orderings, no wall-clock inputs.

pub const HEAD_MAX_ENTRIES: usize = 50;
pub const HEAD_MAX_BYTES: usize = 16384;
const METADATA_RESERVE_BYTES: usize = 2048;
const MAX_HEADER_COUNT: usize = 16;
const MAX_HEADER_VALUE_CHARS: usize = 256;
const MAX_GRPC_JSON_CHARS: usize = 4096;
const MIDDLE_SCAN_LIMIT: i64 = 5_000_000;
const NEW_CONTENT_WINDOW: i64 = 500;

/// Failure to build a transcript. NoContent is a skip, not a failure:
/// content deleted by retention between scheduling and read lands here.
#[derive(Debug)]
pub enum BuildError {
    NoContent,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for BuildError {
    fn from(e: sqlx::Error) -> Self {
        BuildError::Db(e)
    }
}

/// Stream metadata the scheduler already fetched (one row of `stream`).
#[derive(Debug, Clone)]
pub struct StreamContext {
    pub id: i64,
    pub agent_id: Option<String>,
    pub service_id: i64,
    pub service_name: Option<String>,
    pub protocol: String,
    pub l7_protocol: String,
    pub src_ip: String,
    pub src_port: i32,
    pub dst_ip: String,
    pub dst_port: i32,
    pub active: bool,
    pub packet_count: i64,
    pub first_packet_time: i64,
    pub last_packet_time: i64,
    /// Current revision boundary (stream.last_packet_id).
    pub revision_packet_id: i64,
    /// Highest packet id covered by the stream's previous verdict for the
    /// current model + prompt version (0 = first analysis).
    pub after_packet_id: i64,
}

#[derive(Debug)]
pub struct Transcript {
    /// The JEV `state` document: metadata + entries + omission marker.
    pub state: Value,
    /// SHA-256 over the canonical state serialization.
    pub sha256: String,
    pub first_packet_id: i64,
    pub last_packet_id: i64,
    pub analyzed_packet_count: i64,
    pub omitted_packet_count: i64,
    pub omitted_byte_count: i64,
    pub truncated: bool,
    /// Length of the serialized state: the conservative input estimate
    /// (chars/4) for the cost guard.
    pub state_chars: usize,
}

#[derive(Debug, Clone)]
struct PacketRow {
    id: i64,
    timestamp: i64,
    ts_us: i64,
    direction: String,
    data: Vec<u8>,
}

fn packet_row(row: &sqlx::postgres::PgRow) -> PacketRow {
    PacketRow {
        id: row.get("id"),
        timestamp: row.get("timestamp"),
        ts_us: row.get("timestamp_microseconds"),
        direction: row.get("direction"),
        data: row.get("packet_data"),
    }
}

const PACKET_COLUMNS: &str =
    "id, timestamp, timestamp_microseconds, direction, stream_id, packet_data";

/// Packets of one stream, newest-first (tail candidates), bounded per
/// branch so the union never materializes the whole stream. The live +
/// saved union deduplicates by original id, preferring the live copy.
async fn fetch_window(
    pool: &PgPool,
    stream_id: i64,
    limit: i64,
    ascending: bool,
) -> Result<Vec<PacketRow>, sqlx::Error> {
    let order = if ascending { "ASC" } else { "DESC" };
    let sql = format!(
        r#"
        SELECT {cols} FROM (
            SELECT DISTINCT ON (id) id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
            FROM (
                (
                    SELECT {cols}, 0 AS src
                    FROM packets WHERE stream_id = $1
                    ORDER BY timestamp {order}, timestamp_microseconds {order}, id {order}
                    LIMIT $2
                )
                UNION ALL
                (
                    SELECT {cols}, 1 AS src
                    FROM saved_packet WHERE stream_id = $1
                    ORDER BY timestamp {order}, timestamp_microseconds {order}, id {order}
                    LIMIT $2
                )
            ) u
            ORDER BY id, src
        ) d
        ORDER BY timestamp {order}, timestamp_microseconds {order}, id {order}
        LIMIT $2
        "#,
        cols = PACKET_COLUMNS,
        order = order
    );
    let rows = sqlx::query(&sql)
        .bind(stream_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(packet_row).collect())
}

/// True when at least one content-bearing packet (payload or L7 message)
/// exists after `after_packet_id`. Pure-ACK packets never satisfy this,
/// so they advance activity without triggering calls. Scans only the
/// newest rows per branch: anything after `after_packet_id` IS among the
/// newest rows.
pub async fn has_new_analyzable_content(
    pool: &PgPool,
    stream_id: i64,
    after_packet_id: i64,
) -> Result<bool, sqlx::Error> {
    let sql = r#"
        SELECT
            EXISTS (
                SELECT 1 FROM (
                    (
                        SELECT packet_data FROM packets
                        WHERE stream_id = $1 AND id > $2
                        ORDER BY timestamp DESC, timestamp_microseconds DESC, id DESC
                        LIMIT $3
                    )
                    UNION ALL
                    (
                        SELECT packet_data FROM saved_packet
                        WHERE stream_id = $1 AND id > $2
                        ORDER BY timestamp DESC, timestamp_microseconds DESC, id DESC
                        LIMIT $3
                    )
                ) b WHERE length(b.packet_data) > 0
            ) OR EXISTS (
                SELECT 1 FROM (
                    (SELECT 1 FROM http_message WHERE stream_id = $1 AND packet_id > $2 LIMIT 1)
                    UNION ALL
                    (SELECT 1 FROM saved_http_message WHERE stream_id = $1 AND packet_id > $2 LIMIT 1)
                ) h
            )
        "#;
    sqlx::query_scalar::<_, bool>(sql)
        .bind(stream_id)
        .bind(after_packet_id)
        .bind(NEW_CONTENT_WINDOW)
        .fetch_one(pool)
        .await
}

struct HttpRow {
    id: i64,
    packet_id: i64,
    is_request: bool,
    method: Option<String>,
    path: Option<String>,
    status_code: Option<i16>,
    http_version: Option<String>,
    content_type: Option<String>,
    content_length: Option<i32>,
    body_offset: Option<i32>,
    body_length: Option<i32>,
    header_set_id: Option<i64>,
}

/// HTTP messages of the stream, capture order ascending, live + saved
/// union (same shape as the L7 view's query).
async fn fetch_http_messages(pool: &PgPool, stream_id: i64) -> Result<Vec<HttpRow>, sqlx::Error> {
    let sql = r#"
        SELECT id, packet_id, is_request, method, path, status_code, http_version,
               content_type, content_length, body_offset, body_length, header_set_id
        FROM (
            SELECT DISTINCT ON (id) id, packet_id, is_request, method, path, status_code,
                   http_version, content_type, content_length, body_offset, body_length,
                   header_set_id
            FROM (
                SELECT id, packet_id, is_request, method, path, status_code, http_version,
                       content_type, content_length, body_offset, body_length, header_set_id, 0 AS src
                FROM http_message WHERE stream_id = $1
                UNION ALL
                SELECT id, packet_id, is_request, method, path, status_code, http_version,
                       content_type, content_length, body_offset, body_length, header_set_id, 1 AS src
                FROM saved_http_message WHERE stream_id = $1
            ) u
            ORDER BY id, src
        ) d
        ORDER BY timestamp ASC, id ASC
        "#;
    let rows = sqlx::query(sql).bind(stream_id).fetch_all(pool).await?;
    Ok(rows
        .iter()
        .map(|r| HttpRow {
            id: r.get("id"),
            packet_id: r.get("packet_id"),
            is_request: r.get("is_request"),
            method: r.get("method"),
            path: r.get("path"),
            status_code: r.get("status_code"),
            http_version: r.get("http_version"),
            content_type: r.get("content_type"),
            content_length: r.get("content_length"),
            body_offset: r.get("body_offset"),
            body_length: r.get("body_length"),
            header_set_id: r.get("header_set_id"),
        })
        .collect())
}

struct GrpcRow {
    http_message_id: i64,
    service_name: Option<String>,
    method_name: Option<String>,
    is_request: bool,
    decoded_json: String,
}

async fn fetch_grpc_messages(pool: &PgPool, stream_id: i64) -> Result<Vec<GrpcRow>, sqlx::Error> {
    let sql = r#"
        SELECT http_message_id, service_name, method_name, is_request, decoded_json
        FROM (
            SELECT DISTINCT ON (http_message_id) http_message_id, service_name, method_name,
                   is_request, decoded_json
            FROM (
                SELECT g.http_message_id, g.service_name, g.method_name, g.is_request,
                       COALESCE(b.decoded_json, '{}') AS decoded_json, 0 AS src
                FROM grpc_message g
                LEFT JOIN grpc_body_set b ON b.id = g.body_set_id
                WHERE g.stream_id = $1
                UNION ALL
                SELECT g.http_message_id, g.service_name, g.method_name, g.is_request,
                       COALESCE(b.decoded_json, '{}') AS decoded_json, 1 AS src
                FROM saved_grpc_message g
                LEFT JOIN grpc_body_set b ON b.id = g.body_set_id
                WHERE g.stream_id = $1
            ) u
            ORDER BY http_message_id, src
        ) d
        "#;
    let rows = sqlx::query(sql).bind(stream_id).fetch_all(pool).await?;
    Ok(rows
        .iter()
        .map(|r| GrpcRow {
            http_message_id: r.get("http_message_id"),
            service_name: r.get("service_name"),
            method_name: r.get("method_name"),
            is_request: r.get("is_request"),
            decoded_json: r.get("decoded_json"),
        })
        .collect())
}

/// Selected headers of one message, bounded and deterministic (sorted
/// names, capped values).
fn selected_headers(headers: &serde_json::Value) -> Value {
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(obj) = headers.as_object() {
        for (name, value) in obj {
            let rendered = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let capped: String = rendered.chars().take(MAX_HEADER_VALUE_CHARS).collect();
            pairs.push((name.clone(), capped));
            if pairs.len() >= MAX_HEADER_COUNT {
                break;
            }
        }
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut map = Map::new();
    for (name, value) in pairs {
        map.insert(name, Value::String(value));
    }
    Value::Object(map)
}

async fn headers_for_message(pool: &PgPool, header_set_id: Option<i64>) -> Value {
    let Some(id) = header_set_id else {
        return json!({});
    };
    let value: Option<Value> = sqlx::query_scalar("SELECT headers FROM header_set WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
    value.unwrap_or_else(|| json!({}))
}

/// lossy UTF-8 with visible replacement for text-like bytes, hexadecimal
/// for binary. Heuristic (replacement-char and control-char ratios) is
/// deterministic for the same bytes. Hex expansion counts against the
/// transcript budget because the accounting works on the ENCODED form.
fn render_bytes(bytes: &[u8]) -> (&'static str, String) {
    if bytes.is_empty() {
        return ("text", String::new());
    }
    let lossy = String::from_utf8_lossy(bytes);
    let replacements = lossy.matches('\u{FFFD}').count();
    let control = lossy
        .chars()
        .filter(|c| c.is_control() && *c != '\t' && *c != '\r' && *c != '\n')
        .count();
    let text_like = replacements * 16 < bytes.len() && control * 4 < lossy.chars().count();
    if text_like {
        ("text", lossy.into_owned())
    } else {
        ("hex", hex_encode(bytes))
    }
}

/// Hexadecimal expansion counts against the transcript budget because the
/// accounting works on the ENCODED form.
fn hex_encode(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

/// Incremental lossy UTF-8 decode bounded by an encoded-length budget:
/// stops before the char that would exceed the cap. Invalid bytes become
/// one visible U+FFFD each. Used for truncated entries so the included
/// byte count is EXACT (how many original bytes the excerpt represents).
fn decode_lossy_prefix(bytes: &[u8], max_encoded: usize) -> (usize, String) {
    fn decode_one(bytes: &[u8], i: usize) -> (char, usize) {
        let lead = bytes[i];
        let len = match lead {
            0x00..=0x7F => 1,
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => return ('\u{FFFD}', 1),
        };
        if i + len <= bytes.len()
            && let Ok(s) = std::str::from_utf8(&bytes[i..i + len])
        {
            return (s.chars().next().unwrap_or('\u{FFFD}'), len);
        }
        ('\u{FFFD}', 1)
    }

    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let (ch, adv) = decode_one(bytes, i);
        if out.len() + ch.len_utf8() > max_encoded {
            break;
        }
        out.push(ch);
        i += adv;
    }
    (i, out)
}

/// Per-entry payload: capped to entry_max_bytes with explicit original and
/// included byte counts. `included_bytes` always counts ORIGINAL bytes
/// the excerpt represents (hex keeps 2 chars per byte, text keeps
/// decoded chars).
fn payload_entry(bytes: &[u8], entry_max_bytes: usize) -> Value {
    let original = bytes.len();
    let (format, rendered) = render_bytes(bytes);
    if rendered.len() <= entry_max_bytes {
        return json!({"format": format, "data": rendered, "original_bytes": original});
    }
    if format == "hex" {
        let keep = (entry_max_bytes / 2).min(original);
        return json!({
            "format": "hex",
            "data": hex_encode(&bytes[..keep]),
            "original_bytes": original,
            "included_bytes": keep,
            "truncated": true,
        });
    }
    let (included, data) = decode_lossy_prefix(bytes, entry_max_bytes);
    json!({
        "format": "text",
        "data": data,
        "original_bytes": original,
        "included_bytes": included,
        "truncated": true,
    })
}

fn entry_encoded_len(entry: &Value) -> usize {
    serde_json::to_string(entry).map(|s| s.len()).unwrap_or(0)
}

/// Budget fitting for a pre-rendered window (head or tail). Entries keep
/// capture order; an entry that would overflow the byte or count budget
/// is cut. `stop_at_first_overflow` keeps the head a contiguous prefix
/// (initial context), while the tail may skip an oversized entry and keep
/// smaller later ones.
#[derive(Debug)]
struct FittedWindow {
    entries: Vec<Value>,
    used_bytes: usize,
    cut: usize,
}

fn fit_entries(
    entries: Vec<Value>,
    max_count: usize,
    max_bytes: usize,
    stop_at_first_overflow: bool,
) -> FittedWindow {
    let total = entries.len();
    let mut kept = Vec::new();
    let mut used = 0usize;
    let mut cut = 0usize;
    for e in entries.into_iter() {
        let len = entry_encoded_len(&e);
        if kept.len() >= max_count || used + len > max_bytes {
            if stop_at_first_overflow {
                // Head stays a prefix: everything from the first overflow
                // is out.
                cut = total - kept.len();
                break;
            }
            cut += 1;
            continue;
        }
        used += len;
        kept.push(e);
    }
    FittedWindow {
        entries: kept,
        used_bytes: used,
        cut,
    }
}

/// Packet/byte totals of the omitted middle (counts + boundaries only,
/// never payloads). Bounded by the (stream_id, timestamp) index via the
/// boundary timestamps; the id range keeps the slice exact.
async fn middle_stats(
    pool: &PgPool,
    stream_id: i64,
    after_id: i64,
    before_id: i64,
    from_ts: i64,
    to_ts: i64,
) -> Result<(i64, i64), sqlx::Error> {
    let sql = format!(
        r#"
        SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(length(packet_data)), 0)
        FROM (
            (
                SELECT packet_data FROM packets
                WHERE stream_id = $1 AND id > $2 AND id < $3
                  AND timestamp >= $4 AND timestamp <= $5
                LIMIT {lim}
            )
            UNION ALL
            (
                SELECT packet_data FROM saved_packet
                WHERE stream_id = $1 AND id > $2 AND id < $3
                  AND timestamp >= $4 AND timestamp <= $5
                LIMIT {lim}
            )
        ) m
        "#,
        lim = MIDDLE_SCAN_LIMIT
    );
    let row = sqlx::query_as::<_, (i64, i64)>(&sql)
        .bind(stream_id)
        .bind(after_id)
        .bind(before_id)
        .bind(from_ts)
        .bind(to_ts)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

/// Build the bounded transcript for one stream revision.
pub async fn build(
    pool: &PgPool,
    ctx: &StreamContext,
    max_bytes: usize,
    max_entries: usize,
    entry_max_bytes: usize,
) -> Result<Transcript, BuildError> {
    // Tail candidates: the most recent entries through the current
    // revision. Head candidates: the first entries of the stream.
    let tail = fetch_window(pool, ctx.id, max_entries as i64, false).await?;
    let mut tail = tail;
    tail.reverse(); // capture order ascending

    if tail.is_empty() {
        return Err(BuildError::NoContent);
    }

    // No NEW analyzable content since the previous identity -> skip (this
    // covers pure-ACK-only progress and content deleted by retention).
    let has_new = has_new_analyzable_content(pool, ctx.id, ctx.after_packet_id).await?;
    if !has_new {
        return Err(BuildError::NoContent);
    }

    // L7 messages, mapped onto their packets.
    let http_rows = fetch_http_messages(pool, ctx.id).await?;
    let grpc_rows = fetch_grpc_messages(pool, ctx.id).await?;
    let mut grpc_by_http: std::collections::HashMap<i64, &GrpcRow> =
        std::collections::HashMap::new();
    for g in &grpc_rows {
        grpc_by_http.entry(g.http_message_id).or_insert(g);
    }
    let mut http_by_packet: std::collections::HashMap<i64, Vec<&HttpRow>> =
        std::collections::HashMap::new();
    for h in &http_rows {
        http_by_packet.entry(h.packet_id).or_default().push(h);
    }
    // Header sets are shared/deduplicated: resolve each distinct id once.
    let mut headers_cache: std::collections::HashMap<i64, Value> = std::collections::HashMap::new();
    let mut needed: Vec<i64> = http_rows.iter().filter_map(|h| h.header_set_id).collect();
    needed.sort_unstable();
    needed.dedup();
    for id in &needed {
        headers_cache.insert(*id, headers_for_message(pool, Some(*id)).await);
    }

    // Window split: does the tail cover the whole stream?
    let covered_by_tail = tail.len() < max_entries;
    let head_packets: Vec<PacketRow> = if covered_by_tail {
        tail.iter().take(HEAD_MAX_ENTRIES).cloned().collect()
    } else {
        fetch_window(pool, ctx.id, HEAD_MAX_ENTRIES as i64, true).await?
    };

    // Capture-order boundaries for the middle omission.
    let tail_first = tail.first().expect("tail non-empty");
    let head_last_id = head_packets.last().map(|p| p.id).unwrap_or(0);
    let head_last_ts = head_packets.last().map(|p| p.timestamp).unwrap_or(0);
    let middle_exists = !covered_by_tail
        && head_packets
            .last()
            .map(|h| {
                (h.timestamp, h.ts_us, h.id)
                    < (tail_first.timestamp, tail_first.ts_us, tail_first.id)
            })
            .unwrap_or(false);

    let (omitted_count, omitted_bytes) = if middle_exists {
        middle_stats(
            pool,
            ctx.id,
            head_last_id,
            tail_first.id,
            head_last_ts,
            tail_first.timestamp,
        )
        .await?
    } else {
        (0, 0)
    };

    // Head rendering: up to HEAD_MAX_ENTRIES entries AND HEAD_MAX_BYTES of
    // encoded content, kept as a contiguous prefix. Entries that do not
    // fit are dropped from the head (they remain covered by the omission
    // accounting when not in tail).
    let mut head_rendered = Vec::with_capacity(head_packets.len());
    for pkt in &head_packets {
        head_rendered.push(
            render_entry(
                pkt,
                &http_by_packet,
                &grpc_by_http,
                &headers_cache,
                entry_max_bytes,
            )
            .await,
        );
    }
    let head_fit = fit_entries(head_rendered, HEAD_MAX_ENTRIES, HEAD_MAX_BYTES, true);
    let mut head_entries = head_fit.entries;
    let head_bytes = head_fit.used_bytes;
    let head_cut = head_packets.len() - head_entries.len();

    // Tail rendering: newest entries through the revision within the
    // remaining budget.
    let tail_budget = max_bytes.saturating_sub(head_bytes + METADATA_RESERVE_BYTES);
    let mut tail_rendered = Vec::with_capacity(tail.len());
    for pkt in tail.iter().rev() {
        tail_rendered.push(
            render_entry(
                pkt,
                &http_by_packet,
                &grpc_by_http,
                &headers_cache,
                entry_max_bytes,
            )
            .await,
        );
    }
    let tail_fit = fit_entries(
        tail_rendered,
        max_entries.saturating_sub(head_entries.len()),
        tail_budget,
        false,
    );
    let mut tail_entries = tail_fit.entries;
    let tail_cut = tail_fit.cut;
    tail_entries.reverse(); // capture order ascending

    // The tail may have swallowed part of the head window (overlap);
    // head entries whose packet id also appears in the tail are dropped
    // from the head list to avoid double representation.
    let mut tail_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for e in &tail_entries {
        if let Some(id) = e.get("packet_id").and_then(|v| v.as_i64()) {
            tail_ids.insert(id);
        }
    }
    head_entries.retain(|e| {
        !e.get("packet_id")
            .and_then(|v| v.as_i64())
            .map(|id| tail_ids.contains(&id))
            .unwrap_or(false)
    });

    let head_included = head_entries.len() as i64;
    let tail_included = tail_entries.len() as i64;
    let analyzed = head_included + tail_included;

    // Omission accounting. Middle: everything between the head window end
    // and the tail window start. Window cuts (head/tail budget drops) are
    // inside the windows - they add to the omitted totals as pure counts
    // (their bytes are unknown without another scan, and 0 is honest: the
    // budget drop is a truncation, flagged separately).
    let total_omitted = ctx.packet_count.saturating_sub(analyzed).max(0);
    let truncated = middle_exists && omitted_count > 0
        || head_cut > 0
        || tail_cut > 0
        || total_omitted > omitted_count;

    let first_packet_id = head_entries
        .first()
        .or_else(|| tail_entries.first())
        .and_then(|e| e.get("packet_id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(tail_first.id);
    let last_packet_id = tail_entries
        .last()
        .or_else(|| head_entries.last())
        .and_then(|e| e.get("packet_id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(tail_first.id);

    let mut entries = head_entries;
    if middle_exists && omitted_count > 0 {
        entries.push(json!({
            "omission_marker": format!(
                "[{} packets / {} bytes omitted between packet {} ({}) and packet {} ({})]",
                omitted_count,
                omitted_bytes,
                head_last_id,
                format_ts(head_last_ts),
                tail_first.id,
                format_ts(tail_first.timestamp)
            )
        }));
    }
    entries.extend(tail_entries);

    let state = json!({
        "stream": {
            "id": ctx.id,
            "agent_id": ctx.agent_id,
            "service_id": ctx.service_id,
            "service": ctx.service_name,
            "protocol": ctx.protocol,
            "l7_protocol": ctx.l7_protocol,
            "src": format!("{}:{}", ctx.src_ip, ctx.src_port),
            "dst": format!("{}:{}", ctx.dst_ip, ctx.dst_port),
            "active": ctx.active,
            "first_packet_time": ctx.first_packet_time,
            "last_packet_time": ctx.last_packet_time,
            "first_packet_id": first_packet_id,
            "last_packet_id": last_packet_id,
            "revision_packet_id": ctx.revision_packet_id,
            "total_packet_count": ctx.packet_count,
            "analyzed_packet_count": analyzed,
            "omitted_packet_count": total_omitted,
            "omitted_byte_count": omitted_bytes,
            "truncated": truncated,
        },
        "entries": entries,
    });

    let state_string = serde_json::to_string(&state).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(state_string.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(Transcript {
        state,
        sha256,
        first_packet_id,
        last_packet_id,
        analyzed_packet_count: analyzed,
        omitted_packet_count: total_omitted,
        omitted_byte_count: omitted_bytes,
        truncated,
        state_chars: state_string.len(),
    })
}

fn format_ts(ts: i64) -> String {
    // Capture timestamps are Unix seconds; render compact UTC text so the
    // marker stays readable in prompts and logs.
    let secs = ts.max(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("+{}d {:02}:{:02}:{:02}", days, h, m, s)
}

/// One transcript entry per content-bearing packet. Pure-ACK packets
/// (empty payload, no L7) are skipped here: they advance activity but add
/// no analyzable content.
async fn render_entry(
    pkt: &PacketRow,
    http_by_packet: &std::collections::HashMap<i64, Vec<&HttpRow>>,
    grpc_by_http: &std::collections::HashMap<i64, &GrpcRow>,
    headers_cache: &std::collections::HashMap<i64, Value>,
    entry_max_bytes: usize,
) -> Value {
    let mut entry = json!({
        "packet_id": pkt.id,
        "ts": pkt.timestamp,
        "dir": if pkt.direction == "dest" { "inbound" } else { "outbound" },
    });
    let obj = entry.as_object_mut().expect("entry object");

    let messages = http_by_packet.get(&pkt.id);
    if let Some(messages) = messages {
        for h in messages {
            let mut http_obj = Map::new();
            http_obj.insert("is_request".to_string(), json!(h.is_request));
            if let Some(m) = &h.method {
                http_obj.insert("method".to_string(), json!(m));
            }
            if let Some(p) = &h.path {
                http_obj.insert("path".to_string(), json!(p));
            }
            if let Some(s) = h.status_code {
                http_obj.insert("status".to_string(), json!(s));
            }
            if let Some(v) = &h.http_version {
                http_obj.insert("version".to_string(), json!(v));
            }
            if let Some(c) = &h.content_type {
                http_obj.insert("content_type".to_string(), json!(c));
            }
            if let Some(headers) = h.header_set_id.and_then(|id| headers_cache.get(&id)) {
                http_obj.insert("headers".to_string(), selected_headers(headers));
            }

            // Bounded body excerpt through the existing body-offset
            // semantics: body_length bytes at body_offset inside the
            // ORIGINAL packet bytes.
            if let (Some(off), Some(len)) = (h.body_offset, h.body_length)
                && len > 0
                && off >= 0
                && (off as usize) <= pkt.data.len()
            {
                let start = off as usize;
                let avail = pkt.data.len() - start;
                let take = (len as usize).min(avail).min(entry_max_bytes);
                let slice = &pkt.data[start..start + take];
                http_obj.insert("body".to_string(), payload_entry(slice, entry_max_bytes));
                if let Some(cl) = h.content_length {
                    http_obj.insert("content_length".to_string(), json!(cl));
                }
            }

            if let Some(g) = grpc_by_http.get(&h.id) {
                let decoded: String = g.decoded_json.chars().take(MAX_GRPC_JSON_CHARS).collect();
                http_obj.insert(
                    "grpc".to_string(),
                    json!({
                        "service": g.service_name,
                        "method": g.method_name,
                        "is_request": g.is_request,
                        "decoded_json": decoded,
                    }),
                );
            }
            obj.insert("http".to_string(), Value::Object(http_obj));
        }
    } else if !pkt.data.is_empty() {
        // Parsing unavailable or nothing parsed for this packet: the raw
        // payload IS the content (text fallback or hex for binary).
        obj.insert(
            "payload".to_string(),
            payload_entry(&pkt.data, entry_max_bytes),
        );
    }
    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_bytes_render_as_hex_text_as_lossy() {
        let (fmt, hexed) = render_bytes(&[0x00, 0xff, 0xfe, 0x7f, 0x80, 0x01]);
        assert_eq!(fmt, "hex");
        assert_eq!(hexed, "00fffe7f8001");

        let (fmt, text) = render_bytes("GET / HTTP/1.1\r\nHost: x\r\n".as_bytes());
        assert_eq!(fmt, "text");
        assert!(text.starts_with("GET /"));

        // Mostly-ASCII with one bad byte: lossy text with a visible
        // replacement character, no panic.
        let mut bytes = b"hello world this is plain text".to_vec();
        bytes.push(0xff);
        let (fmt, text) = render_bytes(&bytes);
        assert_eq!(fmt, "text");
        assert!(text.contains('\u{FFFD}'));
    }

    #[test]
    fn empty_bytes_render_as_empty_text() {
        let (fmt, text) = render_bytes(&[]);
        assert_eq!(fmt, "text");
        assert!(text.is_empty());
    }

    #[test]
    fn payload_entry_caps_encoded_bytes_and_counts_original() {
        let long = vec![b'a'; 100];
        let entry = payload_entry(&long, 10);
        assert_eq!(entry["truncated"], true);
        assert_eq!(entry["original_bytes"], 100);
        assert_eq!(entry["included_bytes"], 10);
        assert_eq!(entry["data"].as_str().unwrap().len(), 10);

        let short = b"ok".to_vec();
        let entry = payload_entry(&short, 10);
        assert!(entry.get("truncated").is_none());
        assert_eq!(entry["original_bytes"], 2);
    }

    #[test]
    fn payload_entry_hex_expansion_counts_encoded() {
        // 100 binary bytes -> 200 hex chars; cap 150 keeps 150 encoded
        // chars (= 75 original bytes), never the full 200.
        let binary: Vec<u8> = (0..100u8).collect();
        let entry = payload_entry(&binary, 150);
        assert_eq!(entry["truncated"], true);
        assert_eq!(entry["original_bytes"], 100);
        assert_eq!(entry["included_bytes"], 75);
        assert_eq!(entry["data"].as_str().unwrap().len(), 150);
    }

    #[test]
    fn payload_entry_text_cut_respects_char_boundaries() {
        // A multi-byte character straddling the cut must not panic.
        let text = "a".repeat(9) + "\u{00e9}" + &"b".repeat(20); // 2-byte char at 10
        let bytes = text.as_bytes();
        let entry = payload_entry(bytes, 10);
        assert_eq!(entry["truncated"], true);
        let data = entry["data"].as_str().unwrap();
        assert!(data.is_char_boundary(data.len()));
        assert!(data.ends_with('a'));
    }

    #[test]
    fn selected_headers_is_sorted_and_capped() {
        let headers = json!({
            "zebra": "z",
            "alpha": "a",
            "middle": "m"
        });
        let out = selected_headers(&headers);
        let keys: Vec<&String> = out.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["alpha", "middle", "zebra"]);

        let long_value = "x".repeat(1000);
        let out = selected_headers(&json!({"h": long_value}));
        assert_eq!(out["h"].as_str().unwrap().len(), MAX_HEADER_VALUE_CHARS);
    }

    #[test]
    fn head_budget_constants_match_the_spec() {
        assert_eq!(HEAD_MAX_ENTRIES, 50);
        assert_eq!(HEAD_MAX_BYTES, 16384);
    }

    fn entry_of(id: i64, size: usize) -> Value {
        json!({"packet_id": id, "fill": "x".repeat(size)})
    }

    #[test]
    fn fit_entries_respects_count_and_byte_budgets() {
        let entries: Vec<Value> = (0..10).map(|i| entry_of(i, 100)).collect();
        let fit = fit_entries(entries, 5, 10_000, false);
        assert_eq!(fit.entries.len(), 5);
        assert_eq!(fit.cut, 5);
        assert_eq!(fit.used_bytes, 5 * entry_encoded_len(&entry_of(0, 100)));
        assert_eq!(fit.entries[0]["packet_id"], 0);
        assert_eq!(fit.entries[4]["packet_id"], 4);
    }

    #[test]
    fn fit_entries_byte_budget_stops_before_overflow() {
        let entries: Vec<Value> = (0..10).map(|i| entry_of(i, 100)).collect();
        // Enough count for all, bytes for 3.
        let unit = entry_encoded_len(&entry_of(0, 100));
        let fit = fit_entries(entries, 100, unit * 3, false);
        assert_eq!(fit.entries.len(), 3);
        assert_eq!(fit.cut, 7);
    }

    #[test]
    fn fit_entries_head_stays_a_contiguous_prefix() {
        // Entry 2 is oversized; a head (prefix mode) stops there, a tail
        // (skip mode) keeps the smaller entries after it.
        let entries = vec![
            entry_of(0, 10),
            entry_of(1, 10),
            entry_of(2, 500),
            entry_of(3, 10),
        ];
        let unit = entry_encoded_len(&entry_of(0, 10));
        let head = fit_entries(entries.clone(), 10, unit * 2, true);
        assert_eq!(head.entries.len(), 2);
        assert_eq!(head.cut, 2); // entry 2 and 3

        let tail = fit_entries(entries, 10, unit * 3, false);
        assert_eq!(tail.entries.len(), 3); // skips the oversized middle
        assert_eq!(tail.cut, 1);
    }

    #[test]
    fn fit_entries_exact_budget_is_not_a_cut() {
        let entries: Vec<Value> = (0..3).map(|i| entry_of(i, 100)).collect();
        let total: usize = entries.iter().map(entry_encoded_len).sum();
        let fit = fit_entries(entries, 3, total, true);
        assert_eq!(fit.entries.len(), 3);
        assert_eq!(fit.cut, 0);
        assert_eq!(fit.used_bytes, total);
    }
}
