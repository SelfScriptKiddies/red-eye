use crate::api::packet::packet_dto::PacketResponse;
use crate::api::packet::packet_service::PacketService;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use strum_macros::Display;

#[derive(Debug, Serialize, Deserialize, Display, sqlx::Type)]
#[sqlx(type_name = "VARCHAR")]
pub enum Protocol {
    TCP,
    UDP,
}
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Stream {
    pub id: i64,
    pub src_ip: IpAddr,
    pub src_port: i32,
    pub dst_ip: IpAddr,
    pub dst_port: i32,
    pub protocol: Protocol,
    pub active: bool,
    pub packet_count: i32,
    pub favorite: bool,
    pub l7_protocol: String,
    /// Capturing agent (per-agent attribution); None for legacy rows
    #[sqlx(default)]
    pub agent_id: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct StreamResponse {
    pub id: i64,
    pub src_ip: String,
    pub src_port: i32,
    pub dst_ip: String,
    pub dst_port: i32,
    pub protocol: String,
    pub active: bool,
    pub packet_count: i32,
    pub favorite: bool,
    pub l7_protocol: String,
    /// Capturing agent (per-agent attribution); None for legacy rows
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub packets: Vec<PacketResponse>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct StreamListResponse {
    pub id: i64,
    pub service_id: i64,
    pub service_name: String,
    pub tags: Vec<String>,
    pub first_packet_time: i64,
    pub last_packet_time: i64,
    pub packet_count: i32,
    pub favorite: bool,
    pub l7_protocol: String,
    pub src_ip: String,
    pub dst_ip: String,
    /// Capturing agent (per-agent attribution); None for legacy rows
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistogramBucket {
    pub timestamp: i64,
    /// Total packets in the bucket (all services).
    pub packet_count: i64,
    /// Flag tag triggers in the bucket (tag_name.action = 'flag').
    pub flag_count: i64,
}

impl Stream {
    pub(crate) async fn to_response(&self, service: &PacketService) -> StreamResponse {
        StreamResponse {
            id: self.id,
            src_ip: self.src_ip.to_string(),
            src_port: self.src_port,
            dst_ip: self.dst_ip.to_string(),
            dst_port: self.dst_port,
            protocol: self.protocol.to_string(),
            active: self.active,
            packet_count: self.packet_count,
            favorite: self.favorite,
            l7_protocol: self.l7_protocol.clone(),
            agent_id: self.agent_id.clone(),
            packets: service
                .get_packets_by_stream_id(self.id)
                .await
                .unwrap_or(vec![])
                .iter()
                .map(|packet| packet.to_response())
                .collect(),
        }
    }
}
