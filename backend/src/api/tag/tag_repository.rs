use sqlx::PgPool;
use crate::api::tag::tag_dto::{Tag, TagName};

pub struct TagRepository {
    pool: PgPool,
}

impl TagRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_tag_name(&self, name: &str) -> Result<TagName, sqlx::Error> {
        sqlx::query_as!(
            TagName,
            "INSERT INTO tag_name (name) VALUES ($1) RETURNING id, name",
            name
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn create_tag(&self, packet_id: i32, tag_id: i32, start_index: i32, end_index: i32) -> Result<Tag, sqlx::Error> {
        sqlx::query_as!(
            Tag,
            "INSERT INTO tag (packet_id, tag_id, start_index, end_index) 
             VALUES ($1, $2, $3, $4) 
             RETURNING id, packet_id, tag_id, start_index, end_index",
            packet_id,
            tag_id,
            start_index,
            end_index
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_tags_by_packet_id(&self, packet_id: i32) -> Result<Vec<Tag>, sqlx::Error> {
        sqlx::query_as!(
            Tag,
            "SELECT id, packet_id, tag_id, start_index, end_index 
             FROM tag 
             WHERE packet_id = $1",
            packet_id
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_tag_name_by_id(&self, id: i32) -> Result<TagName, sqlx::Error> {
        sqlx::query_as!(
            TagName,
            "SELECT id, name FROM tag_name WHERE id = $1",
            id
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn delete_tag(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "DELETE FROM tag WHERE id = $1",
            id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_tag_name(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "DELETE FROM tag_name WHERE id = $1",
            id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}