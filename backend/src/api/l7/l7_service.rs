use sqlx::{Error, PgPool};

use super::l7_dto::{HttpHeader, HttpMessage, HttpMessageFull};
use super::l7_repository::L7Repository;

pub struct L7Service {
    repository: L7Repository,
}

impl L7Service {
    pub fn new_from_pgpool(pgpool: PgPool) -> Self {
        Self {
            repository: L7Repository::new(pgpool),
        }
    }

    /// Get all HTTP messages for a stream (without headers)
    pub async fn get_messages_by_stream(
        &self,
        stream_id: i32,
    ) -> Result<Vec<HttpMessage>, Error> {
        self.repository.get_messages_by_stream(stream_id).await
    }

    /// Get a specific message with all its headers
    pub async fn get_message_full(&self, message_id: i32) -> Result<HttpMessageFull, Error> {
        let message = self.repository.get_message_by_id(message_id).await?;
        let headers = self.repository.get_headers_by_message(message_id).await?;
        Ok(HttpMessageFull { message, headers })
    }

    /// Get headers for a message
    pub async fn get_headers(&self, message_id: i32) -> Result<Vec<HttpHeader>, Error> {
        self.repository.get_headers_by_message(message_id).await
    }

    /// Search streams by a single header
    pub async fn search_by_header(
        &self,
        name: &str,
        value: Option<&str>,
        limit: i64,
    ) -> Result<(Vec<i32>, i64), Error> {
        let stream_ids = self
            .repository
            .search_streams_by_header(name, value, limit)
            .await?;
        let total = self
            .repository
            .count_header_matches(name, value)
            .await?;
        Ok((stream_ids, total))
    }

    /// Search streams by multiple headers (AND)
    pub async fn search_by_headers(
        &self,
        filters: &[(String, Option<String>)],
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        self.repository
            .search_streams_by_headers(filters, limit)
            .await
    }
}
