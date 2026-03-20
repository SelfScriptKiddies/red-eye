use sqlx::{Error, PgPool, Row};

use super::l7_dto::{HttpHeader, HttpMessage};

pub struct L7Repository {
    pgpool: PgPool,
}

impl L7Repository {
    pub fn new(pgpool: PgPool) -> Self {
        Self { pgpool }
    }

    /// Get all HTTP messages for a stream
    pub async fn get_messages_by_stream(
        &self,
        stream_id: i32,
    ) -> Result<Vec<HttpMessage>, Error> {
        sqlx::query_as::<_, HttpMessage>(
            "SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length, timestamp
             FROM http_message
             WHERE stream_id = $1
             ORDER BY timestamp ASC, id ASC",
        )
        .bind(stream_id)
        .fetch_all(&self.pgpool)
        .await
    }

    /// Get a specific HTTP message by ID
    pub async fn get_message_by_id(&self, id: i32) -> Result<HttpMessage, Error> {
        sqlx::query_as::<_, HttpMessage>(
            "SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length, timestamp
             FROM http_message
             WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&self.pgpool)
        .await
    }

    /// Get headers for a specific message
    pub async fn get_headers_by_message(
        &self,
        message_id: i32,
    ) -> Result<Vec<HttpHeader>, Error> {
        sqlx::query_as::<_, HttpHeader>(
            "SELECT id, message_id, name, value
             FROM http_header
             WHERE message_id = $1
             ORDER BY id ASC",
        )
        .bind(message_id)
        .fetch_all(&self.pgpool)
        .await
    }

    /// Search streams by header name and optional value (AND logic for multiple calls)
    pub async fn search_streams_by_header(
        &self,
        name: &str,
        value: Option<&str>,
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        if let Some(val) = value {
            sqlx::query_scalar::<_, i32>(
                "SELECT DISTINCT m.stream_id
                 FROM http_header h
                 JOIN http_message m ON m.id = h.message_id AND m.timestamp = h.timestamp
                 WHERE LOWER(h.name) = LOWER($1) AND h.value = $2
                 ORDER BY m.stream_id DESC
                 LIMIT $3",
            )
            .bind(name)
            .bind(val)
            .bind(limit)
            .fetch_all(&self.pgpool)
            .await
        } else {
            sqlx::query_scalar::<_, i32>(
                "SELECT DISTINCT m.stream_id
                 FROM http_header h
                 JOIN http_message m ON m.id = h.message_id AND m.timestamp = h.timestamp
                 WHERE LOWER(h.name) = LOWER($1)
                 ORDER BY m.stream_id DESC
                 LIMIT $2",
            )
            .bind(name)
            .bind(limit)
            .fetch_all(&self.pgpool)
            .await
        }
    }

    /// Search streams by multiple headers (AND logic)
    pub async fn search_streams_by_headers(
        &self,
        filters: &[(String, Option<String>)],
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        // Build intersected query: each filter narrows the result set
        let mut result: Option<Vec<i32>> = None;

        for (name, value) in filters {
            let ids = self
                .search_streams_by_header(name, value.as_deref(), limit)
                .await?;

            result = Some(match result {
                None => ids,
                Some(prev) => prev.into_iter().filter(|id| ids.contains(id)).collect(),
            });
        }

        Ok(result.unwrap_or_default())
    }

    /// Count total headers matching criteria (for stats)
    pub async fn count_header_matches(
        &self,
        name: &str,
        value: Option<&str>,
    ) -> Result<i64, Error> {
        if let Some(val) = value {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(DISTINCT m.stream_id)
                 FROM http_header h
                 JOIN http_message m ON m.id = h.message_id AND m.timestamp = h.timestamp
                 WHERE LOWER(h.name) = LOWER($1) AND h.value = $2",
            )
            .bind(name)
            .bind(val)
            .fetch_one(&self.pgpool)
            .await
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(DISTINCT m.stream_id)
                 FROM http_header h
                 JOIN http_message m ON m.id = h.message_id AND m.timestamp = h.timestamp
                 WHERE LOWER(h.name) = LOWER($1)",
            )
            .bind(name)
            .fetch_one(&self.pgpool)
            .await
        }
    }
}
