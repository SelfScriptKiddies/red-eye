use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct TagName {
    pub id: i32,
    pub name: String
}

#[derive(sqlx::FromRow, Deserialize, Serialize, utoipa::ToSchema)]
pub struct Tag {
    pub id: i32,
    pub name: String,
    pub packet_id: i32,
    pub tag_id: i32,
    pub start_index: i32,
    pub end_index: i32
}

