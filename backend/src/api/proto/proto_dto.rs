use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct ProtoFile {
    pub id: i32,
    pub service_id: i32,
    pub filename: String,
    pub content: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ProtoFileCreate {
    pub service_id: i32,
    pub filename: String,
    pub content: String,
}
