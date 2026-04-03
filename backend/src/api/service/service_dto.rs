#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct Service {
    pub id: i32,
    pub name: String,
    pub port: i32,
    pub chunked_transfer_encoding: bool,
    pub urldecode: bool,
    pub merge_adjacent_packets: bool,
    pub inflate_websockets: bool,
    pub decrypt_tls: bool,
    pub http_parse: bool,
    pub http2_parse: bool
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ServiceTransfer {
    pub name: String,
    pub port: i32,
    pub chunked_transfer_encoding: Option<bool>,
    pub urldecode: Option<bool>,
    pub merge_adjacent_packets: Option<bool>,
    pub inflate_websockets: Option<bool>,
    pub decrypt_tls: Option<bool>,
    pub http_parse: Option<bool>,
    pub http2_parse: Option<bool>
}

impl From<ServiceTransfer> for Service {
    fn from(transfer: ServiceTransfer) -> Self {
        Service {
            id: 0,
            name: transfer.name,
            port: transfer.port,
            chunked_transfer_encoding: transfer.chunked_transfer_encoding.unwrap_or(false),
            urldecode: transfer.urldecode.unwrap_or(false),
            merge_adjacent_packets: transfer.merge_adjacent_packets.unwrap_or(false),
            inflate_websockets: transfer.inflate_websockets.unwrap_or(false),
            decrypt_tls: transfer.decrypt_tls.unwrap_or(false),
            http_parse: transfer.http_parse.unwrap_or(false),
            http2_parse: transfer.http2_parse.unwrap_or(false)
        }
    }
}