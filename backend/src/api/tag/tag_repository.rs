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
            "INSERT INTO tag_name (name, color, template_type, template, direction, special_type)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id, name, color, template_type, template, direction, special_type"
        )
        .bind(tag_name.name)
        .bind(tag_name.color)
        .bind(tag_name.template_type)
        .bind(tag_name.template)
        .bind(tag_name.direction)
        .bind(tag_name.special_type)
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
            "SELECT id, name, color, template_type, template, direction, special_type
             FROM tag_name WHERE id = $1"
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_tag_name(&self, id: i32, tag_name: TagNameCreate) -> Result<TagName, sqlx::Error> {
        sqlx::query_as::<_, TagName>(
            "UPDATE tag_name SET name = $1, color = $2, template_type = $3, template = $4, direction = $5, special_type = $6
             WHERE id = $7
             RETURNING id, name, color, template_type, template, direction, special_type"
        )
        .bind(tag_name.name)
        .bind(tag_name.color)
        .bind(tag_name.template_type)
        .bind(tag_name.template)
        .bind(tag_name.direction)
        .bind(tag_name.special_type)
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn delete_tag(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM tag WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_tag_name(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM tag_name WHERE id = $1")
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
            "SELECT id, name, color, template_type, template, direction, special_type
             FROM tag_name ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Get tag trigger counts aggregated by minute for the last N minutes
    pub async fn get_tag_metrics(&self, minutes: i32) -> Result<Vec<(i32, String, i64, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i32, String, i64, i64)>(
            "SELECT tn.id, tn.name,
                    (t.timestamp / 60) * 60 as minute_ts,
                    COUNT(*) as trigger_count
             FROM tag t
             JOIN tag_name tn ON tn.id = t.tag_id
             WHERE t.timestamp >= (EXTRACT(EPOCH FROM NOW())::BIGINT - ($1 * 60))
             GROUP BY tn.id, tn.name, minute_ts
             ORDER BY minute_ts ASC"
        )
        .bind(minutes)
        .fetch_all(&self.pool)
        .await
    }
}
