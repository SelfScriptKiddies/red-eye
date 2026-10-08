use base64::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Packet {
    pub id: i64,
    pub timestamp: i64,
    pub timestamp_microseconds: i64,
    pub direction: String,
    pub stream_id: i64,
    pub packet_data: Vec<u8>, // BYTEA PostgreSQL
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct PacketResponse {
    pub id: i64,
    pub timestamp: i64,
    pub timestamp_microseconds: i64,
    pub direction: String,
    pub stream_id: i64,
    pub packet_data: String, // base64 string
}

impl Packet {
    pub fn to_response(&self) -> PacketResponse {
        let packet_data_base64 = BASE64_STANDARD.encode(&self.packet_data);

        PacketResponse {
            id: self.id,
            timestamp: self.timestamp,
            timestamp_microseconds: self.timestamp_microseconds,
            direction: self.direction.clone(),
            stream_id: self.stream_id,
            packet_data: packet_data_base64,
        }
    }
}