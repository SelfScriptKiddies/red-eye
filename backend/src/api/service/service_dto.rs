#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct Packet {
    pub id: i32,
    pub name: String,
    pub port: i32,
    pub chunked_transfer_encoding: bool,
    pub urldecode: bool,
    pub merge_adjacent_packets: bool,
    pub inflate_websockets: bool,
    pub decrypt_tls: bool
}