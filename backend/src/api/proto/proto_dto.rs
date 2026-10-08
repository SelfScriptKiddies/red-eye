use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct ProtoFile {
    pub id: i64,
    pub service_id: i64,
    pub filename: String,
    pub content: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ProtoFileCreate {
    pub service_id: i64,
    pub filename: String,
    pub content: String,
}
