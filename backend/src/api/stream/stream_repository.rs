use sqlx::PgPool;
use crate::api::stream::stream_dto::Stream;

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
}


