use actix_web::{HttpRequest, HttpResponse, Responder, get, web};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

use crate::api::soc::bearer;
use crate::api::soc::settings::MCP_SERVICE_TOKEN;

// Bounded read-only resource endpoints for red-eye-mcp. The MCP service
// holds no database credentials: every tool it exposes translates into
// one of these calls. All limits clamp server-side, projections are
// explicit and payload-free where the tool contract says so.

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;
const DEFAULT_ENTRY_BYTES: i64 = 4096;
const MAX_ENTRY_BYTES: i64 = 16384;
const DEFAULT_PACKET_BYTES: i64 = 8192;
const MAX_PACKET_BYTES: i64 = 65536;
const RELATED_MAX_WINDOW_SECS: i64 = 3600;
const RELATED_MAX_LIMIT: i64 = 20;

#[get("/mcp/services")]
async fn list_services(pool: web::Data<PgPool>, req: HttpRequest) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    match sqlx::query_as::<_, (i64, String, i32, String, bool)>(
        "SELECT id, name, port, protocol, is_global FROM service ORDER BY id ASC LIMIT 200",
    )
    .fetch_all(pool.get_ref())
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|(id, name, port, protocol, is_global)| {
                    json!({"id": id, "name": name, "port": port, "protocol": protocol, "is_global": is_global})
                })
                .collect::<Vec<_>>(),
        ),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[get("/mcp/agents")]
async fn list_agents(
    pool: web::Data<PgPool>,
    agent_service: web::Data<std::sync::Arc<crate::api::agent::agent_service::AgentService>>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let agents = agent_service.list_agents().await;
    HttpResponse::Ok().json(
        agents
            .iter()
            .map(|a| {
                json!({
                    "agent_id": a.agent_id,
                    "online": a.online,
                    "machine_ip": a.machine_ip,
                })
            })
            .collect::<Vec<_>>(),
    )
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct StreamsQuery {
    service_id: Option<i64>,
    agent_id: Option<String>,
    tag_id: Option<i64>,
    favorite: Option<bool>,
    /// Bare or CIDR-styled IP matched against src or dst.
    ip: Option<String>,
    protocol: Option<String>,
    from_secs: Option<i64>,
    to_secs: Option<i64>,
    limit: Option<i64>,
    cursor_time: Option<i64>,
    cursor_id: Option<i64>,
}

/// GET /api/mcp/streams: keyset pagination over the same ordering the UI
/// list uses ((last_time, id) DESC). Bounded filters only; no full-text
/// or regex search over contents.
#[get("/mcp/streams")]
async fn list_streams(
    pool: web::Data<PgPool>,
    web::Query(q): web::Query<StreamsQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let cursor_time = q.cursor_time.unwrap_or(i64::MAX);
    let cursor_id = q.cursor_id.unwrap_or(i64::MAX);

    let mut sql = String::from(
        r#"
        SELECT s.id, s.src_ip::text, s.src_port, s.dst_ip::text, s.dst_port, s.protocol,
               s.active, s.favorite, s.packet_count::bigint AS packet_count,
               s.l7_protocol, s.agent_id, s.service_id, sv.name AS service_name,
               COALESCE(s.first_packet_time, 0) AS first_time,
               COALESCE(s.last_packet_time, 0) AS last_time,
               COALESCE(s.last_packet_id, 0) AS last_packet_id
        FROM stream s JOIN service sv ON sv.id = s.service_id
        WHERE (COALESCE(s.last_packet_time, 0), s.id) < ($1, $2)
        "#,
    );
    let mut n = 3;
    for (cond, present) in [
        (" AND s.service_id = ${n}", q.service_id.is_some()),
        (" AND s.agent_id = ${n}", q.agent_id.is_some()),
        (" AND s.favorite = ${n}", q.favorite.is_some()),
        (
            " AND (s.src_ip::text LIKE ${n} || '%' OR s.dst_ip::text LIKE ${n} || '%')",
            q.ip.is_some(),
        ),
        (" AND s.protocol = ${n}", q.protocol.is_some()),
        (
            " AND COALESCE(s.last_packet_time, 0) >= ${n}",
            q.from_secs.is_some(),
        ),
        (
            " AND COALESCE(s.last_packet_time, 0) <= ${n}",
            q.to_secs.is_some(),
        ),
        (
            " AND EXISTS (SELECT 1 FROM stream_tag st JOIN tag_name tn ON tn.name = st.tag_name \
             WHERE st.stream_id = s.id AND tn.id = ${n})",
            q.tag_id.is_some(),
        ),
    ] {
        if present {
            sql.push_str(&cond.replace("${n}", &n.to_string()));
            n += 1;
        }
    }
    sql.push_str(&format!(
        " ORDER BY COALESCE(s.last_packet_time, 0) DESC, s.id DESC LIMIT ${}",
        n
    ));

    let mut query = sqlx::query_as::<
        _,
        (
            i64,
            String,
            i32,
            String,
            i32,
            String,
            bool,
            bool,
            i64,
            String,
            Option<String>,
            i64,
            String,
            i64,
            i64,
            i64,
        ),
    >(&sql)
    .bind(cursor_time)
    .bind(cursor_id);
    if let Some(v) = q.service_id {
        query = query.bind(v);
    }
    if let Some(v) = &q.agent_id {
        query = query.bind(v.clone());
    }
    if let Some(v) = q.favorite {
        query = query.bind(v);
    }
    if let Some(v) = &q.ip {
        // Normalize "10.0.0.1/32" (UI-styled) and reject injection-shaped
        // input by keeping only prefix-searchable address text.
        let bare = v.trim().split('/').next().unwrap_or("").to_string();
        let bare: String = bare
            .chars()
            .filter(|c| c.is_ascii_hexdigit() || *c == '.' || *c == ':')
            .collect();
        query = query.bind(bare);
    }
    if let Some(v) = &q.protocol {
        query = query.bind(v.to_uppercase());
    }
    if let Some(v) = q.from_secs {
        query = query.bind(v);
    }
    if let Some(v) = q.to_secs {
        query = query.bind(v);
    }
    if let Some(v) = q.tag_id {
        query = query.bind(v);
    }
    match query.bind(limit).fetch_all(pool.get_ref()).await {
        Ok(rows) => HttpResponse::Ok().json(rows.iter().map(stream_json).collect::<Vec<_>>()),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

const STREAM_PROJECTION: &str = r#"
    SELECT s.id, s.src_ip::text, s.src_port, s.dst_ip::text, s.dst_port, s.protocol,
           s.active, s.favorite, s.packet_count::bigint AS packet_count,
           s.l7_protocol, s.agent_id, s.service_id, sv.name AS service_name,
           COALESCE(s.first_packet_time, 0) AS first_time,
           COALESCE(s.last_packet_time, 0) AS last_time,
           COALESCE(s.last_packet_id, 0) AS last_packet_id
    FROM stream s JOIN service sv ON sv.id = s.service_id
    WHERE s.id = $1
"#;

type StreamRow = (
    i64,
    String,
    i32,
    String,
    i32,
    String,
    bool,
    bool,
    i64,
    String,
    Option<String>,
    i64,
    String,
    i64,
    i64,
    i64,
);

fn stream_json(r: &StreamRow) -> serde_json::Value {
    json!({
        "id": r.0, "src_ip": r.1, "src_port": r.2, "dst_ip": r.3, "dst_port": r.4,
        "protocol": r.5, "active": r.6, "favorite": r.7, "packet_count": r.8,
        "l7_protocol": r.9, "agent_id": r.10, "service_id": r.11,
        "service_name": r.12, "first_packet_time": r.13, "last_packet_time": r.14,
        "last_packet_id": r.15,
    })
}

/// GET /api/mcp/stream/{id}/summary: tuple, service, agent, counts, tags,
/// favorite and segment metadata plus the latest JEV verdict and triage
/// verdict - WITHOUT payload material.
#[get("/mcp/stream/{id}/summary")]
async fn stream_summary(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let stream_id = path.into_inner();
    let row: Option<StreamRow> = sqlx::query_as(STREAM_PROJECTION)
        .bind(stream_id)
        .fetch_optional(pool.get_ref())
        .await
        .unwrap_or(None);
    let Some(r) = row else {
        return HttpResponse::NotFound().json(json!({"error": "stream not found"}));
    };

    let tags: Vec<serde_json::Value> = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT tn.id, tn.name FROM stream_tag st JOIN tag_name tn ON tn.name = st.tag_name
        WHERE st.stream_id = $1 ORDER BY tn.name LIMIT 50
        "#,
    )
    .bind(stream_id)
    .fetch_all(pool.get_ref())
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(id, name)| json!({"id": id, "name": name}))
    .collect();

    let jev: Option<(String, f64, bool, i64, i64)> = sqlx::query_as(
        r#"
        SELECT attack_class, severity::float8, is_attack, last_packet_id, id
        FROM ai_verdict WHERE stream_id = $1
        ORDER BY last_packet_id DESC, id DESC LIMIT 1
        "#,
    )
    .bind(stream_id)
    .fetch_optional(pool.get_ref())
    .await
    .unwrap_or(None);

    let triage: Option<(String, f64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT verdict, confidence::float8, severity, stream_revision
        FROM soc_triage_result WHERE stream_id = $1
        ORDER BY stream_revision DESC, id DESC LIMIT 1
        "#,
    )
    .bind(stream_id)
    .fetch_optional(pool.get_ref())
    .await
    .unwrap_or(None);

    HttpResponse::Ok().json(json!({
        "stream": stream_json(&r),
        "tags": tags,
        "segment": {
            "segment_index": segment_index(pool.get_ref(), stream_id).await,
        },
        "latest_jev": jev.map(|(class, sev, attack, rev, id)| json!({
            "id": id, "attack_class": class, "severity": sev,
            "is_attack": attack, "last_packet_id": rev,
        })),
        "latest_triage": triage.map(|(verdict, conf, sev, rev)| json!({
            "verdict": verdict, "confidence": conf, "severity": sev,
            "stream_revision": rev,
        })),
    }))
}

async fn segment_index(pool: &PgPool, stream_id: i64) -> i32 {
    sqlx::query_scalar::<_, i32>("SELECT segment_index FROM stream WHERE id = $1")
        .bind(stream_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(1)
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct MessagesQuery {
    limit: Option<i64>,
    max_entry_bytes: Option<i64>,
}

/// GET /api/mcp/stream/{id}/messages: bounded chronological excerpts,
/// two-direction, raw + L7 with explicit truncation marks. Never full
/// rows; every entry is byte-capped.
#[get("/mcp/stream/{id}/messages")]
async fn stream_messages(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    web::Query(q): web::Query<MessagesQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let stream_id = path.into_inner();
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let max_entry = q
        .max_entry_bytes
        .unwrap_or(DEFAULT_ENTRY_BYTES)
        .clamp(128, MAX_ENTRY_BYTES);

    // Raw excerpts: bounded byte slices ordered chronologically.
    let raw: Vec<serde_json::Value> = sqlx::query_as::<_, (i64, String, i64, Vec<u8>)>(
        r#"
        SELECT id, direction, timestamp, substring(packet_data from 1 for $3)
        FROM packets WHERE stream_id = $1
        ORDER BY timestamp ASC, id ASC LIMIT $2
        "#,
    )
    .bind(stream_id)
    .bind(limit)
    .bind(max_entry as i32 + 1)
    .fetch_all(pool.get_ref())
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(id, direction, ts, data)| {
        let full = data.len() as i64 > max_entry;
        let mut bytes = data;
        bytes.truncate(max_entry.max(0) as usize);
        json!({
            "kind": "packet",
            "id": id,
            "direction": direction,
            "timestamp": ts,
            "text_preview": text_preview(&bytes),
            "hex_preview": hex_preview(&bytes),
            "truncated": full,
        })
    })
    .collect();

    let l7: Vec<serde_json::Value> =
        sqlx::query_as::<_, (i64, bool, Option<String>, Option<String>, Option<i16>, i64)>(
            r#"
        SELECT id, is_request, method, path, status_code, timestamp
        FROM http_message WHERE stream_id = $1
        ORDER BY timestamp ASC, id ASC LIMIT $2
        "#,
        )
        .bind(stream_id)
        .bind(limit)
        .fetch_all(pool.get_ref())
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(id, is_request, method, path_, status, ts)| {
            json!({
                "kind": "http_message",
                "id": id,
                "is_request": is_request,
                "method": method,
                "path": path_,
                "status_code": status,
                "timestamp": ts,
                "body_omitted": true,
            })
        })
        .collect();

    HttpResponse::Ok().json(json!({
        "stream_id": stream_id,
        "limit": limit,
        "max_entry_bytes": max_entry,
        "packets": raw,
        "http_messages": l7,
    }))
}

fn text_preview(bytes: &[u8]) -> String {
    let s: String = bytes
        .iter()
        .map(|b| {
            if b.is_ascii_graphic() || *b == b' ' || *b == b'\n' || *b == b'\t' || *b == b'\r' {
                *b as char
            } else {
                '.'
            }
        })
        .collect();
    s
}

fn hex_preview(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes.iter().take(1024) {
        s.push_str(&format!("{:02x}", b));
    }
    if bytes.len() > 1024 {
        s.push_str("...");
    }
    s
}

/// GET /api/mcp/packet/{id}: one known packet with bounded text, hex and
/// base64 representations.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct PacketQuery {
    max_bytes: Option<i64>,
}

#[get("/mcp/packet/{id}")]
async fn get_packet(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    web::Query(q): web::Query<PacketQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let max_bytes = q
        .max_bytes
        .unwrap_or(DEFAULT_PACKET_BYTES)
        .clamp(128, MAX_PACKET_BYTES);
    let row: Option<(i64, i64, String, i64, Vec<u8>)> = sqlx::query_as(
        r#"
        SELECT id, stream_id, direction, timestamp, substring(packet_data from 1 for $2)
        FROM packets WHERE id = $1 ORDER BY timestamp DESC LIMIT 1
        "#,
    )
    .bind(id)
    .bind(max_bytes as i32 + 1)
    .fetch_optional(pool.get_ref())
    .await
    .unwrap_or(None);
    let Some((pid, stream_id, direction, ts, data)) = row else {
        return HttpResponse::NotFound().json(json!({"error": "packet not found"}));
    };
    let truncated = data.len() as i64 > max_bytes;
    let mut bytes = data;
    bytes.truncate(max_bytes.max(0) as usize);
    use base64::Engine;
    HttpResponse::Ok().json(json!({
        "id": pid,
        "stream_id": stream_id,
        "direction": direction,
        "timestamp": ts,
        "size_capped": bytes.len(),
        "truncated": truncated,
        "text": text_preview(&bytes),
        "hex": hex_preview(&bytes),
        "base64": base64::engine::general_purpose::STANDARD.encode(&bytes),
    }))
}

/// GET /api/mcp/message/{id}/headers: one known HTTP message's headers,
/// byte-capped. Header sets are JSONB in header_set.
#[get("/mcp/message/{id}/headers")]
async fn get_message_headers(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    web::Query(q): web::Query<PacketQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let max_bytes = q
        .max_bytes
        .unwrap_or(DEFAULT_ENTRY_BYTES)
        .clamp(128, MAX_ENTRY_BYTES);
    let row: Option<(i64, i64, Option<i64>)> = sqlx::query_as(
        "SELECT id, stream_id, header_set_id FROM http_message WHERE id = $1 ORDER BY timestamp DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(pool.get_ref())
    .await
    .unwrap_or(None);
    let Some((mid, stream_id, header_set_id)) = row else {
        return HttpResponse::NotFound().json(json!({"error": "message not found"}));
    };
    let Some(hs_id) = header_set_id else {
        return HttpResponse::Ok().json(json!({
            "id": mid, "stream_id": stream_id, "headers": {}, "note": "no headers recorded",
        }));
    };
    let headers: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT headers FROM header_set WHERE id = $1")
            .bind(hs_id)
            .fetch_optional(pool.get_ref())
            .await
            .unwrap_or(None);
    let serialized = headers.clone().map(|h| h.to_string()).unwrap_or_default();
    let truncated = serialized.len() > max_bytes as usize;
    HttpResponse::Ok().json(json!({
        "id": mid,
        "stream_id": stream_id,
        "headers": if truncated { serde_json::Value::Null } else { headers.unwrap_or(serde_json::Value::Null) },
        "truncated": truncated,
        "max_bytes": max_bytes,
    }))
}

/// GET /api/mcp/stream/{id}/tags
#[get("/mcp/stream/{id}/tags")]
async fn stream_tags(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let stream_id = path.into_inner();
    match sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT tn.id, tn.name FROM stream_tag st JOIN tag_name tn ON tn.name = st.tag_name
        WHERE st.stream_id = $1 ORDER BY tn.name LIMIT 50
        "#,
    )
    .bind(stream_id)
    .fetch_all(pool.get_ref())
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|(id, name)| json!({"id": id, "name": name}))
                .collect::<Vec<_>>(),
        ),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/mcp/stream/{id}/jev-verdicts?limit=: typed answers and
/// revisions, never provider credentials.
#[get("/mcp/stream/{id}/jev-verdicts")]
async fn stream_jev_verdicts(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    web::Query(q): web::Query<MessagesQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    match sqlx::query_as::<_, (i64, i64, i64, String, f64, f64, bool, serde_json::Value)>(
        r#"
        SELECT id, first_packet_id, last_packet_id, attack_class, severity::float8,
               confidence::float8, is_attack, answers
        FROM ai_verdict WHERE stream_id = $1
        ORDER BY last_packet_id DESC, id DESC LIMIT $2
        "#,
    )
    .bind(path.into_inner())
    .bind(limit)
    .fetch_all(pool.get_ref())
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|(id, first, last, class, sev, conf, attack, answers)| {
                    json!({
                        "id": id, "first_packet_id": first, "last_packet_id": last,
                        "attack_class": class, "severity": sev, "confidence": conf,
                        "is_attack": attack, "answers": answers,
                    })
                })
                .collect::<Vec<_>>(),
        ),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct RelatedQuery {
    stream_id: i64,
    window_secs: Option<i64>,
    limit: Option<i64>,
}

/// GET /api/mcp/related-streams: same remote IP + service + agent within
/// a REQUIRED bounded window. Summaries only, never payloads.
#[get("/mcp/related-streams")]
async fn related_streams(
    pool: web::Data<PgPool>,
    web::Query(q): web::Query<RelatedQuery>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let window = q
        .window_secs
        .unwrap_or(300)
        .clamp(1, RELATED_MAX_WINDOW_SECS);
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, RELATED_MAX_LIMIT);
    let base: Option<StreamRow> = sqlx::query_as(STREAM_PROJECTION)
        .bind(q.stream_id)
        .fetch_optional(pool.get_ref())
        .await
        .unwrap_or(None);
    let Some(base) = base else {
        return HttpResponse::NotFound().json(json!({"error": "stream not found"}));
    };
    // The "remote" side is the initiating endpoint (src).
    let sql = format!(
        r#"
        WITH base AS ({})
        SELECT s.id, s.src_ip::text, s.src_port, s.dst_ip::text, s.dst_port, s.protocol,
               s.active, s.favorite, s.packet_count::bigint AS packet_count,
               s.l7_protocol, s.agent_id, s.service_id, sv.name AS service_name,
               COALESCE(s.first_packet_time, 0), COALESCE(s.last_packet_time, 0),
               COALESCE(s.last_packet_id, 0)
        FROM stream s JOIN service sv ON sv.id = s.service_id, base b
        WHERE s.id <> b.id
          AND s.src_ip = b.src_ip AND s.service_id = b.service_id
          AND COALESCE(s.agent_id, '') = COALESCE(b.agent_id, '')
          AND ABS(COALESCE(s.last_packet_time, 0) - COALESCE(b.last_packet_time, 0)) <= $2
        ORDER BY ABS(COALESCE(s.last_packet_time, 0) - COALESCE(b.last_packet_time, 0)) ASC, s.id ASC
        LIMIT $3
        "#,
        STREAM_PROJECTION
    );
    match sqlx::query_as::<_, StreamRow>(&sql)
        .bind(base.0)
        .bind(window)
        .bind(limit)
        .fetch_all(pool.get_ref())
        .await
    {
        Ok(rows) => HttpResponse::Ok().json(json!({
            "base_stream_id": base.0,
            "window_secs": window,
            "streams": rows.iter().map(stream_json).collect::<Vec<_>>(),
        })),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/mcp/soc-result/{id}: validated triage output; no hidden
/// reasoning exists to expose.
#[get("/mcp/soc-result/{id}")]
async fn soc_result(
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = guard(pool.get_ref(), &req).await {
        return r;
    }
    let repo = crate::api::soc::repository::SocRepository::new(pool.get_ref().clone());
    match repo.get_result(path.into_inner()).await {
        Ok(Some(row)) => HttpResponse::Ok().json(row),
        Ok(None) => HttpResponse::NotFound().json(json!({"error": "result not found"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/mcp/health: operator-safe status without secrets.
#[get("/mcp/health")]
async fn component_health(
    pool: web::Data<PgPool>,
    health: web::Data<std::sync::Arc<crate::health::HealthMonitor>>,
) -> impl Responder {
    let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool.get_ref())
        .await
        .is_ok();
    HttpResponse::Ok().json(json!({
        "db": db_ok,
        "health_state": health.state(),
        "p95_ms": health.p95_ms(),
    }))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(list_services)
        .service(list_agents)
        .service(list_streams)
        .service(stream_summary)
        .service(stream_messages)
        .service(get_packet)
        .service(get_message_headers)
        .service(stream_tags)
        .service(stream_jev_verdicts)
        .service(related_streams)
        .service(soc_result)
        .service(component_health);
}

/// Service-credential guard shared by every resource endpoint. Failures
/// fail closed; a store outage surfaces as 5xx, never as open access.
async fn guard(pool: &PgPool, req: &HttpRequest) -> Result<(), HttpResponse> {
    bearer::require_service_token(pool, req, MCP_SERVICE_TOKEN, Some("mcp_service_last_seen")).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_preview_replaces_control_bytes() {
        let s = text_preview(b"GET /x\x00\x01 HTTP");
        assert!(s.contains("GET /x.. HTTP"));
        assert!(!s.contains('\u{0}'));
    }

    #[test]
    fn hex_preview_is_capped_and_marks_tail() {
        let s = hex_preview(&vec![0xab_u8; 2000]);
        assert!(s.ends_with("..."));
        assert!(s.len() < 2100);
    }
}
