use sqlx::{Error, PgPool, Row};

use super::l7_dto::HttpMessage;

pub struct L7Repository {
    pgpool: PgPool,
}

impl L7Repository {
    pub fn new(pgpool: PgPool) -> Self {
        Self { pgpool }
    }

    /// HTTP messages of a stream from live AND saved storage, deduplicated
    /// by original id, preserving the (timestamp, id) ordering.
    pub async fn get_messages_by_stream(&self, stream_id: i64) -> Result<Vec<HttpMessage>, Error> {
        sqlx::query_as::<_, HttpMessage>(
            r#"
            SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length,
                    header_set_id, timestamp
            FROM (
                SELECT DISTINCT ON (id) id, stream_id, packet_id, is_request, method, path,
                       status_code, http_version, content_type, content_length, body_offset,
                       body_length, header_set_id, timestamp
                FROM (
                    SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                           http_version, content_type, content_length, body_offset, body_length,
                           header_set_id, timestamp, 0 AS src
                    FROM http_message WHERE stream_id = $1
                    UNION ALL
                    SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                           http_version, content_type, content_length, body_offset, body_length,
                           header_set_id, timestamp, 1 AS src
                    FROM saved_http_message WHERE stream_id = $1
                ) u
                ORDER BY id, src
            ) d
            ORDER BY timestamp ASC, id ASC
            "#,
        )
        .bind(stream_id)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn get_message_by_id(&self, id: i64) -> Result<HttpMessage, Error> {
        sqlx::query_as::<_, HttpMessage>(
            r#"
            SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length,
                    header_set_id, timestamp
            FROM (
                SELECT DISTINCT ON (id) id, stream_id, packet_id, is_request, method, path,
                       status_code, http_version, content_type, content_length, body_offset,
                       body_length, header_set_id, timestamp
                FROM (
                    SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                           http_version, content_type, content_length, body_offset, body_length,
                           header_set_id, timestamp, 0 AS src
                    FROM http_message WHERE id = $1
                    UNION ALL
                    SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                           http_version, content_type, content_length, body_offset, body_length,
                           header_set_id, timestamp, 1 AS src
                    FROM saved_http_message WHERE id = $1
                ) u
                ORDER BY id, src
            ) d
            "#,
        )
        .bind(id)
        .fetch_one(&self.pgpool)
        .await
    }

    /// Get headers JSONB from header_set by message's header_set_id
    /// (message looked up across live and saved storage).
    pub async fn get_headers_by_message(
        &self,
        message_id: i64,
    ) -> Result<serde_json::Value, Error> {
        let result = sqlx::query_scalar::<_, serde_json::Value>(
            r#"
            SELECT hs.headers
            FROM (
                SELECT header_set_id FROM http_message WHERE id = $1
                UNION ALL
                SELECT header_set_id FROM saved_http_message WHERE id = $1
            ) m
            JOIN header_set hs ON hs.id = m.header_set_id
            LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.pgpool)
        .await?;

        Ok(result.unwrap_or(serde_json::json!({})))
    }

    /// Search streams by header name/value using JSONB
    pub async fn search_streams_by_header(
        &self,
        name: &str,
        value: Option<&str>,
        limit: i64,
    ) -> Result<Vec<i64>, Error> {
        if let Some(val) = value {
            // Use LIKE for partial matches (e.g. individual cookies within Cookie header)
            let use_like = name.eq_ignore_ascii_case("Cookie") || val.contains('=');
            if use_like {
                let pattern = format!("%{}%", val);
                sqlx::query_scalar::<_, i64>(
                    "SELECT DISTINCT m.stream_id
                     FROM http_message m
                     JOIN header_set hs ON hs.id = m.header_set_id
                     WHERE hs.headers->>$1 LIKE $2
                     ORDER BY m.stream_id DESC
                     LIMIT $3",
                )
                .bind(name)
                .bind(pattern)
                .bind(limit)
                .fetch_all(&self.pgpool)
                .await
            } else {
                sqlx::query_scalar::<_, i64>(
                    "SELECT DISTINCT m.stream_id
                     FROM http_message m
                     JOIN header_set hs ON hs.id = m.header_set_id
                     WHERE hs.headers->>$1 = $2
                     ORDER BY m.stream_id DESC
                     LIMIT $3",
                )
                .bind(name)
                .bind(val)
                .bind(limit)
                .fetch_all(&self.pgpool)
                .await
            }
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT DISTINCT m.stream_id
                 FROM http_message m
                 JOIN header_set hs ON hs.id = m.header_set_id
                 WHERE hs.headers ? $1
                 ORDER BY m.stream_id DESC
                 LIMIT $2",
            )
            .bind(name)
            .bind(limit)
            .fetch_all(&self.pgpool)
            .await
        }
    }

    /// Search streams by IP address
    pub async fn search_streams_by_ip(&self, ip: &str, limit: i64) -> Result<Vec<i64>, Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT id FROM stream
             WHERE src_ip::text = $1 OR dst_ip::text = $1
             ORDER BY id DESC
             LIMIT $2",
        )
        .bind(ip)
        .bind(limit)
        .fetch_all(&self.pgpool)
        .await
    }

    /// Search streams by HTTP path (supports prefix match with LIKE)
    pub async fn search_streams_by_path(&self, path: &str, limit: i64) -> Result<Vec<i64>, Error> {
        // Use LIKE if path contains %, otherwise exact or prefix match
        let pattern = if path.contains('%') {
            path.to_string()
        } else {
            format!("{}%", path) // prefix match by default
        };
        sqlx::query_scalar::<_, i64>(
            "SELECT DISTINCT stream_id
             FROM http_message
             WHERE path LIKE $1
             ORDER BY stream_id DESC
             LIMIT $2",
        )
        .bind(&pattern)
        .bind(limit)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn search_streams_by_headers(
        &self,
        filters: &[(String, Option<String>)],
        limit: i64,
    ) -> Result<Vec<i64>, Error> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        let mut result: Option<Vec<i64>> = None;

        for (name, value) in filters {
            let ids = if name == "_path" {
                self.search_streams_by_path(value.as_deref().unwrap_or(""), limit)
                    .await?
            } else if name == "_ip" {
                self.search_streams_by_ip(value.as_deref().unwrap_or(""), limit)
                    .await?
            } else {
                self.search_streams_by_header(name, value.as_deref(), limit)
                    .await?
            };

            result = Some(match result {
                None => ids,
                Some(prev) => {
                    let ids_set: std::collections::HashSet<i64> = ids.into_iter().collect();
                    prev.into_iter().filter(|id| ids_set.contains(id)).collect()
                }
            });
        }

        Ok(result.unwrap_or_default())
    }

    pub async fn count_header_matches(
        &self,
        name: &str,
        value: Option<&str>,
    ) -> Result<i64, Error> {
        if let Some(val) = value {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(DISTINCT m.stream_id)
                 FROM http_message m
                 JOIN header_set hs ON hs.id = m.header_set_id
                 WHERE hs.headers->>$1 = $2",
            )
            .bind(name)
            .bind(val)
            .fetch_one(&self.pgpool)
            .await
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(DISTINCT m.stream_id)
                 FROM http_message m
                 JOIN header_set hs ON hs.id = m.header_set_id
                 WHERE hs.headers ? $1",
            )
            .bind(name)
            .fetch_one(&self.pgpool)
            .await
        }
    }

    // ── gRPC ──

    /// gRPC messages of a stream from live AND saved storage, deduplicated
    /// by original id, preserving the (timestamp, id) ordering. Body sets
    /// stay shared; packet_id comes from the http_message parent (live or
    /// saved twin of it).
    pub async fn get_grpc_messages_by_stream(
        &self,
        stream_id: i64,
    ) -> Result<Vec<super::l7_dto::GrpcMessage>, Error> {
        sqlx::query_as::<_, super::l7_dto::GrpcMessage>(
            r#"
            SELECT id, stream_id, http_message_id, service_name, method_name,
                    is_request, body_set_id, timestamp,
                    COALESCE(decoded_json, '{}') as decoded_json,
                    packet_id
            FROM (
                SELECT DISTINCT ON (id) id, stream_id, http_message_id, service_name,
                       method_name, is_request, body_set_id, timestamp,
                       decoded_json, packet_id
                FROM (
                    SELECT g.id, g.stream_id, g.http_message_id, g.service_name, g.method_name,
                           g.is_request, g.body_set_id, g.timestamp,
                           COALESCE(b.decoded_json, '{}') as decoded_json,
                           hm.packet_id, 0 AS src
                    FROM grpc_message g
                    LEFT JOIN grpc_body_set b ON b.id = g.body_set_id
                    LEFT JOIN http_message hm ON hm.id = g.http_message_id
                        AND hm.timestamp = g.timestamp
                    WHERE g.stream_id = $1
                    UNION ALL
                    SELECT g.id, g.stream_id, g.http_message_id, g.service_name, g.method_name,
                           g.is_request, g.body_set_id, g.timestamp,
                           COALESCE(b.decoded_json, '{}') as decoded_json,
                           shm.packet_id, 1 AS src
                    FROM saved_grpc_message g
                    LEFT JOIN grpc_body_set b ON b.id = g.body_set_id
                    LEFT JOIN saved_http_message shm ON shm.id = g.http_message_id
                        AND shm.timestamp = g.timestamp
                    WHERE g.stream_id = $1
                ) u
                ORDER BY id, src
            ) d
            ORDER BY timestamp ASC, id ASC
            "#,
        )
        .bind(stream_id)
        .fetch_all(&self.pgpool)
        .await
    }
}
