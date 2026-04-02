use sqlx::{Error, PgPool};

use super::l7_dto::{HttpMessage, HttpMessageFull};
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

    pub async fn get_messages_by_stream(
        &self,
        stream_id: i32,
    ) -> Result<Vec<HttpMessage>, Error> {
        self.repository.get_messages_by_stream(stream_id).await
    }

    pub async fn get_message_full(&self, message_id: i32) -> Result<HttpMessageFull, Error> {
        let message = self.repository.get_message_by_id(message_id).await?;
        let headers = self.repository.get_headers_by_message(message_id).await?;
        Ok(HttpMessageFull { message, headers })
    }

    pub async fn get_headers(&self, message_id: i32) -> Result<serde_json::Value, Error> {
        self.repository.get_headers_by_message(message_id).await
    }

    pub async fn search_by_header(
        &self,
        name: &str,
        value: Option<&str>,
        limit: i64,
    ) -> Result<(Vec<i32>, i64), Error> {
        let stream_ids = if name == "_path" {
            self.repository
                .search_streams_by_path(value.unwrap_or(""), limit)
                .await?
        } else if name == "_ip" {
            self.repository
                .search_streams_by_ip(value.unwrap_or(""), limit)
                .await?
        } else {
            self.repository
                .search_streams_by_header(name, value, limit)
                .await?
        };
        let total = stream_ids.len() as i64;
        Ok((stream_ids, total))
    }

    pub async fn search_by_headers(
        &self,
        filters: &[(String, Option<String>)],
        limit: i64,
    ) -> Result<Vec<i32>, Error> {
        self.repository
            .search_streams_by_headers(filters, limit)
            .await
    }

    pub async fn get_grpc_messages(
        &self,
        stream_id: i32,
    ) -> Result<Vec<super::l7_dto::GrpcMessage>, Error> {
        self.repository
            .get_grpc_messages_by_stream(stream_id)
            .await
    }
}
