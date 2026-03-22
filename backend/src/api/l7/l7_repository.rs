use sqlx::{Error, PgPool, Row};

use super::l7_dto::HttpMessage;

pub struct L7Repository {
    pgpool: PgPool,
}

impl L7Repository {
    pub fn new(pgpool: PgPool) -> Self {
        Self { pgpool }
    }

    pub async fn get_messages_by_stream(
        &self,
        stream_id: i32,
    ) -> Result<Vec<HttpMessage>, Error> {
        sqlx::query_as::<_, HttpMessage>(
            "SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length,
                    header_set_id, timestamp
             FROM http_message
             WHERE stream_id = $1
             ORDER BY timestamp ASC, id ASC",
        )
        .bind(stream_id)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn get_message_by_id(&self, id: i32) -> Result<HttpMessage, Error> {
        sqlx::query_as::<_, HttpMessage>(
            "SELECT id, stream_id, packet_id, is_request, method, path, status_code,
                    http_version, content_type, content_length, body_offset, body_length,
                    header_set_id, timestamp
             FROM http_message
             WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&self.pgpool)
        .await
    }

    /// Get headers JSONB from header_set by message's header_set_id
    pub async fn get_headers_by_message(
        &self,
        message_id: i32,
    ) -> Result<serde_json::Value, Error> {
        let result = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT hs.headers
             FROM http_message m
             JOIN header_set hs ON hs.id = m.header_set_id
             WHERE m.id = $1",
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
    ) -> Result<Vec<i32>, Error> {
        if let Some(val) = value {
            sqlx::query_scalar::<_, i32>(
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
        } else {
            sqlx::query_scalar::<_, i32>(
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

    /// Search streams by HTTP path
    pub async fn search_streams_by_path(
        &self,
        path: &str,
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        sqlx::query_scalar::<_, i32>(
            "SELECT DISTINCT stream_id
             FROM http_message
             WHERE path = $1
             ORDER BY stream_id DESC
             LIMIT $2",
        )
        .bind(path)
        .bind(limit)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn search_streams_by_headers(
        &self,
        filters: &[(String, Option<String>)],
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        let mut result: Option<Vec<i32>> = None;

        for (name, value) in filters {
            let ids = if name == "_path" {
                self.search_streams_by_path(value.as_deref().unwrap_or(""), limit).await?
            } else {
                self.search_streams_by_header(name, value.as_deref(), limit).await?
            };

            result = Some(match result {
                None => ids,
                Some(prev) => prev.into_iter().filter(|id| ids.contains(id)).collect(),
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
}
