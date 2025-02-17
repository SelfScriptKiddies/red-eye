use sqlx::PgPool;
use crate::api::tag::tag_dto::{Tag, TagName, TagNameCreate};

pub struct TagRepository {
    pool: PgPool,
}

impl TagRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_tag_name(&self, tag_name: TagNameCreate) -> Result<TagName, sqlx::Error> {
        sqlx::query_as::<_, TagName>(
            "INSERT INTO tag_name (name, color) VALUES ($1, $2) RETURNING id, name, color"
        )
        .bind(tag_name.name)
        .bind(tag_name.color)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_tags_by_packet_id(&self, packet_id: i32) -> Result<Vec<Tag>, sqlx::Error> {
        sqlx::query_as::<_, Tag>(
            "SELECT id, packet_id, tag_id, start_index, end_index 
             FROM tag 
             WHERE packet_id = $1",
        )
        .bind(packet_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_tag_name_by_id(&self, id: i32) -> Result<TagName, sqlx::Error> {
        sqlx::query_as::<_, TagName>(
            "SELECT id, name, color FROM tag_name WHERE id = $1"
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_tag_name(&self, id: i32, tag_name: TagNameCreate) -> Result<TagName, sqlx::Error> {
        sqlx::query_as::<_, TagName>(
            "UPDATE tag_name SET name = $1, color = $2 WHERE id = $3 RETURNING id, name, color"
        )
        .bind(tag_name.name)
        .bind(tag_name.color)
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }   

    pub async fn delete_tag(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM tag WHERE id = $1"
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_tag_name(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM tag_name WHERE id = $1"
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_tags_by_stream_id(&self, stream_id: i32) -> Result<Vec<Tag>, sqlx::Error> {
        sqlx::query_as::<_, Tag>(
            "SELECT t.id, tn.name, tn.color, t.packet_id, t.tag_id, t.start_index, t.end_index 
             FROM tag t
             JOIN packets p ON t.packet_id = p.id
             JOIN tag_name tn ON t.tag_id = tn.id
             WHERE p.stream_id = $1"
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_all_tag_names(&self) -> Result<Vec<TagName>, sqlx::Error> {
        sqlx::query_as::<_, TagName>(
            "SELECT id, name, color FROM tag_name ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await
    }
}