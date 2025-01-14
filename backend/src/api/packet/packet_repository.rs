use sqlx::PgPool;
use crate::api::packet::packet_dto::Packet;

#[derive(Debug, Clone)]
pub(super) struct PacketRepository {
    pgpool: PgPool
}

impl PacketRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        PacketRepository {
            pgpool
        }
    }

    pub async fn get_packet_by_id(&self, packet_id: i32) -> Result<Packet, sqlx::Error> {
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, direction, stream_id, packet_data
            FROM packets
            WHERE id = $1
            "#)
            .bind(packet_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_packet_by_stream_id(&self, stream_id: i32) -> Result<Vec<Packet>, sqlx::Error> {
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, direction, stream_id, packet_data
            FROM packets
            WHERE stream_id = $1
            ORDER BY timestamp
            "#)
            .bind(stream_id)
            .fetch_all(&self.pgpool)
            .await
    }
}