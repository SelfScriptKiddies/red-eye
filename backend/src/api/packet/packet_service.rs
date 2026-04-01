use sqlx::{Error, PgPool};
use crate::api::packet::packet_dto::Packet;
use crate::api::stream::stream_dto::Stream;
use crate::api::packet::packet_repository::PacketRepository;

#[derive(Debug, Clone)]
pub(crate) struct PacketService {
    pub(crate) packet_repository: PacketRepository
}

impl PacketService {
    fn new(packet_repository: PacketRepository) -> Self {
        PacketService {
            packet_repository
        }
    }

    pub(crate) fn new_from_pgpool(pg_pool: PgPool) -> Self {
        PacketService::new(
            PacketRepository::new(pg_pool)
        )
    }

    pub async fn get_packet_by_id(&self, id: i32) -> Result<Packet, sqlx::Error> {
        self.packet_repository.get_packet_by_id(id).await
    }

    pub async fn get_packets_by_stream_id(&self, stream_id: i32) -> Result<Vec<Packet>, Error> {
        self.packet_repository.get_packet_by_stream_id(stream_id, 500).await
    }

    pub async fn get_packets_by_stream_id_limited(&self, stream_id: i32, limit: i32) -> Result<Vec<Packet>, Error> {
        self.packet_repository.get_packet_by_stream_id(stream_id, limit).await
    }

    pub async fn get_packets_by_stream(&self, stream: &Stream) -> Result<Vec<Packet>, Error> {
        self.packet_repository.get_packet_by_stream_id(stream.id, 500).await
    }

    pub async fn get_packets(&self, limit: u32, last_id: Option<u32>) -> Result<Vec<Packet>, Error> {
        self.packet_repository.get_packets(limit, last_id).await
    }
}