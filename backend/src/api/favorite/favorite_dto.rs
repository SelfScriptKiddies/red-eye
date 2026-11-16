use serde::Serialize;
use utoipa::ToSchema;

/// Current retention state of one stream, returned by the favorite endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct FavoriteStateResponse {
    pub id: i64,
    pub favorite: bool,
    pub favorite_revision: i64,
    pub retention_state: String,
    pub segment_index: i32,
    pub retention_bytes: i64,
    pub archived_through_packet_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_error: Option<String>,
}

/// Capacity snapshot for the favorites storage budget. saved_bytes is a
/// catalog estimate of the physical saved_* size; reserved_bytes adds the
/// worst-case remaining growth of active starred segments, closed segments
/// still awaiting archive, and saved content awaiting post-unstar cleanup.
#[derive(Debug, Serialize, ToSchema)]
pub struct FavoriteUsageResponse {
    pub max_total_bytes: i64,
    pub segment_bytes: i64,
    pub disk_reserve_bytes: i64,
    pub saved_bytes: i64,
    pub reserved_bytes: i64,
    pub starred_streams: i64,
    pub active_starred_streams: i64,
    pub pending_cleanup_streams: i64,
}
