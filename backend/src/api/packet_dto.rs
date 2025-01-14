use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Packet {
    pub id: i32,
    pub timestamp: i64,
    pub direction: String,
    pub stream_id: i32,
    pub packet_data: Vec<u8>, // BYTEA PostgreSQL
}

#[derive(Debug, Serialize)]
pub struct PacketResponse {
    pub id: i32,
    pub timestamp: i64,
    pub direction: String,
    pub stream_id: i32,
    pub packet_data: String, // base64 string
}