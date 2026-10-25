use super::proto_dto::ProtoFile;
use sqlx::{Error, PgPool};

pub struct ProtoRepository {
    pool: PgPool,
}

impl ProtoRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_by_service(&self, service_id: i64) -> Result<Vec<ProtoFile>, Error> {
        sqlx::query_as::<_, ProtoFile>(
            "SELECT id, service_id, filename, content FROM proto_file WHERE service_id = $1 ORDER BY filename"
        )
        .bind(service_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_by_id(&self, id: i64) -> Result<ProtoFile, Error> {
        sqlx::query_as::<_, ProtoFile>(
            "SELECT id, service_id, filename, content FROM proto_file WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn create(
        &self,
        service_id: i64,
        filename: &str,
        content: &str,
    ) -> Result<ProtoFile, Error> {
        sqlx::query_as::<_, ProtoFile>(
            "INSERT INTO proto_file (service_id, filename, content) VALUES ($1, $2, $3)
             RETURNING id, service_id, filename, content",
        )
        .bind(service_id)
        .bind(filename)
        .bind(content)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn delete(&self, id: i64) -> Result<(), Error> {
        sqlx::query("DELETE FROM proto_file WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_all(&self) -> Result<Vec<ProtoFile>, Error> {
        sqlx::query_as::<_, ProtoFile>(
            "SELECT id, service_id, filename, content FROM proto_file ORDER BY service_id, filename"
        )
        .fetch_all(&self.pool)
        .await
    }
}
