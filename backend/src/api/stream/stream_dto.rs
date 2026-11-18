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
    // Starred-stream retention metadata (additive fields; see
    // database/migrations/add_favorite_retention.sql).
    #[sqlx(default)]
    pub favorite_revision: i64,
    #[sqlx(default)]
    pub archived_through_packet_id: i64,
    #[sqlx(default)]
    pub retention_state: String,
    #[sqlx(default)]
    pub continuation_of: Option<i64>,
    #[sqlx(default)]
    pub segment_index: i32,
    #[sqlx(default)]
    pub retention_bytes: i64,
    #[sqlx(default)]
    pub archive_error: Option<String>,
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
    // Retention metadata (additive; absent values stay out of the JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_of: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_segment_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_error: Option<String>,
    /// True when historical payload expired before retention existed.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
    pub packets: Vec<PacketResponse>,
}

/// One row of the streams list query (the old 16-tuple became a struct
/// when retention fields joined the projection).
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct StreamListRow {
    pub stream_id: i64,
    pub first_time: Option<i64>,
    pub last_time: Option<i64>,
    pub packet_count: i64,
    pub tags: Vec<String>,
    pub service_name: Option<String>,
    pub service_id: Option<i64>,
    pub favorite: bool,
    pub l7_protocol: String,
    pub src_ip: Option<String>,
    pub dst_ip: Option<String>,
    #[sqlx(default)]
    pub agent_id: Option<String>,
    #[sqlx(default)]
    pub retention_state: String,
    #[sqlx(default)]
    pub segment_index: i32,
    #[sqlx(default)]
    pub retention_bytes: i64,
    #[sqlx(default)]
    pub continuation_of: Option<i64>,
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
    // Retention metadata (additive).
    pub retention_state: String,
    pub segment_index: i32,
    /// Payload-byte estimate of the retained content of this segment.
    pub retention_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_of: Option<i64>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistogramBucket {
    pub timestamp: i64,
    /// Total packets in the bucket (all services).
    pub packet_count: i64,
    /// Flag tag triggers in the bucket (tag_name.special_type = 'flag').
    pub flag_count: i64,
}

impl Stream {
    pub(crate) async fn to_response(
        &self,
        service: &PacketService,
        next_segment_id: Option<i64>,
    ) -> StreamResponse {
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
            retention_state: Some(self.retention_state.clone()),
            segment_index: Some(self.segment_index),
            continuation_of: self.continuation_of,
            next_segment_id,
            retention_bytes: Some(self.retention_bytes),
            archive_error: self.archive_error.clone(),
            incomplete: self.retention_state == "incomplete",
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
