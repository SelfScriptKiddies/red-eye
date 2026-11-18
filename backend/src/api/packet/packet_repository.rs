use crate::api::packet::packet_dto::Packet;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub(super) struct PacketRepository {
    pgpool: PgPool,
}

impl PacketRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        PacketRepository { pgpool }
    }

    pub async fn get_packet_by_id(&self, packet_id: i64) -> Result<Packet, sqlx::Error> {
        // Union live + saved: a starred packet stays readable after its
        // partition is dropped. DISTINCT ON (id) keeps the live copy while
        // both exist (archival window); saved storage failure surfaces as
        // an explicit error, never as a silently missing packet.
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
            FROM (
                SELECT DISTINCT ON (id) id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
                FROM (
                    SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data, 0 AS src
                    FROM packets WHERE id = $1
                    UNION ALL
                    SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data, 1 AS src
                    FROM saved_packet WHERE id = $1
                ) u
                ORDER BY id, src
            ) d
            "#,
        )
        .bind(packet_id)
        .fetch_one(&self.pgpool)
        .await
    }

    pub async fn get_packet_by_stream_id(
        &self,
        stream_id: i64,
        limit: i64,
    ) -> Result<Vec<Packet>, sqlx::Error> {
        // Live + saved union deduplicated by original id, preserving the
        // (timestamp, microseconds, id) ordering the UI relies on. Body
        // offsets keep referencing the original packet bytes (saved rows
        // are verbatim copies).
        sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
            FROM (
                SELECT DISTINCT ON (id) id, timestamp, timestamp_microseconds, direction, stream_id, packet_data
                FROM (
                    SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data, 0 AS src
                    FROM packets WHERE stream_id = $1
                    UNION ALL
                    SELECT id, timestamp, timestamp_microseconds, direction, stream_id, packet_data, 1 AS src
                    FROM saved_packet WHERE stream_id = $1
                ) u
                ORDER BY id, src
            ) d
            ORDER BY timestamp, timestamp_microseconds, id
            LIMIT $2
            "#,
        )
        .bind(stream_id)
        .bind(limit)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn get_packets(
        &self,
        limit: u32,
        last_id: Option<u32>,
    ) -> Result<Vec<Packet>, sqlx::Error> {
        let mut query_builder = sqlx::QueryBuilder::new("SELECT p.* FROM packets p ");

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
