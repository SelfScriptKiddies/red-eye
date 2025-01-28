use sqlx::PgPool;
use crate::api::stream::stream_dto::{Stream, StreamListResponse};

pub struct StreamRepository {
    pgpool: PgPool
}

impl StreamRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        StreamRepository {
            pgpool
        }
    }

    pub async fn get_packet_by_id(&self, stream_id: i32) -> Result<Stream, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count
            FROM stream
            WHERE id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_all_streams(&self) -> Result<Vec<Stream>, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count
            FROM stream
            "#)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn get_stream_packet_timestamps(&self, stream_id: i32) -> Result<(Option<i64>, Option<i64>), sqlx::Error> {
        sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
            r#"
            SELECT MIN(timestamp) as first_time, MAX(timestamp) as last_time
            FROM packets
            WHERE stream_id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_streams_with_tags(&self, limit: i32, offset: i32) -> Result<Vec<(i32, Option<i64>, Option<i64>, Vec<String>)>, sqlx::Error> {
        sqlx::query_as::<_, (i32, Option<i64>, Option<i64>, Vec<String>)>(
            r#"
            SELECT 
                p.stream_id,
                MIN(p.timestamp) as first_time,
                MAX(p.timestamp) as last_time,
                COALESCE(array_agg(DISTINCT tn.name) FILTER (WHERE tn.name IS NOT NULL), ARRAY[]::text[]) as tags
            FROM packets p
            LEFT JOIN tag t ON t.packet_id = p.id
            LEFT JOIN tag_name tn ON tn.id = t.tag_id
            GROUP BY p.stream_id
            ORDER BY last_time
            LIMIT $1
            OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pgpool)
        .await
    }
}


