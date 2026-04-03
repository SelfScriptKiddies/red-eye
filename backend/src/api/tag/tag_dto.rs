use serde::{Deserialize, Serialize};
use crate::api::updater::FilterDirection;

#[derive(sqlx::FromRow, Deserialize, Serialize, utoipa::ToSchema, Clone)]
pub struct TagName {
    pub id: i32,
    pub name: String,
    pub color: i16,
    pub template_type: String,
    pub template: String,
    pub direction: String,
    pub special_type: Option<String>,
}

#[derive(sqlx::FromRow, Deserialize, Serialize, utoipa::ToSchema)]
pub struct TagNameCreate {
    pub name: String,
    pub color: i16,
    pub template_type: String,
    pub template: String,
    pub direction: String,
    pub special_type: Option<String>,
}


/// Triggered tag, so filters found something
#[derive(sqlx::FromRow, Deserialize, Serialize, utoipa::ToSchema)]
pub struct Tag {
    pub id: i32,
    pub name: String,
    pub color: i16,
    pub packet_id: i32,
    pub tag_id: i32,
    pub start_index: i32,
    pub end_index: i32
}

