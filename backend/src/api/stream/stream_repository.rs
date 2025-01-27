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

    pub async fn get_stream_packet_timestamps(&self, stream_id: i32) -> Result<(i64, i64), sqlx::Error> {
        sqlx::query_as::<_, StreamListResponse>(
            r#"
            SELECT MIN(timestamp) as first_time, MAX(timestamp) as last_time
            FROM packets
            WHERE stream_id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }
}


