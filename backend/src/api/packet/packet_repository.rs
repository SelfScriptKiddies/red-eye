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

    pub async fn get_packet_by_id(&self, packet_id: i64) -> Result<Packet, sqlx::Error> {
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
            FROM packets
            WHERE id = $1
            "#)
            .bind(packet_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_packet_by_stream_id(&self, stream_id: i64, limit: i64) -> Result<Vec<Packet>, sqlx::Error> {
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
            FROM packets
            WHERE stream_id = $1
            ORDER BY timestamp, timestamp_microseconds
            LIMIT $2
            "#)
            .bind(stream_id)
            .bind(limit)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn get_packets(&self, limit: u32, last_id: Option<u32>) -> Result<Vec<Packet>, sqlx::Error> {
        let mut query_builder = sqlx::QueryBuilder::new(
            "SELECT p.* FROM packets p "
        );

        if let Some(id) = last_id {
            query_builder
                .push("WHERE p.timestamp < (SELECT timestamp FROM packets WHERE id = ")
                .push_bind(id as i64)
                .push(") ");
        }

        query_builder
            .push("ORDER BY p.timestamp DESC, p.timestamp_microseconds DESC LIMIT ")
            .push_bind(limit as i64);

        query_builder
            .build_query_as::<Packet>()
            .fetch_all(&self.pgpool)
            .await
    }
}