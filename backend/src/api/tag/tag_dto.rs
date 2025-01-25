use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize)]
struct TagName {
    id: i32,
    name: String
}

#[derive(sqlx::FromRow, Deserialize, Serialize)]
struct Tag {
    id: i32,
    packet_id: i32,
    tag_id: i32,
    start_index: i32,
    end_index: i32
}