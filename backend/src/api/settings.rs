use actix_identity::Identity;
use actix_web::{get, put, web, HttpResponse, Responder};
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SettingUpdate {
    pub value: String,
}

#[utoipa::path(get, path = "/api/settings", tag = "settings",
    responses((status = 200, description = "All settings")))]
#[get("/settings")]
async fn get_all_settings(
    _identity: Identity,
    pool: web::Data<PgPool>,
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    match sqlx::query_as::<_, (String, String)>("SELECT key, value FROM app_settings")
        .fetch_all(pool.get_ref())
        .await
    {
        Ok(rows) => {
            let mut map: HashMap<String, String> = rows.into_iter().collect();
            map.entry("max_selected_streams".to_string())
                .or_insert_with(|| config.max_selected_streams.to_string());
            HttpResponse::Ok().json(map)
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(put, path = "/api/settings/{key}", tag = "settings",
    responses((status = 200, description = "Setting updated")))]
#[put("/settings/{key}")]
async fn update_setting(
    _identity: Identity,
    pool: web::Data<PgPool>,
    key: web::Path<String>,
    body: web::Json<SettingUpdate>,
) -> impl Responder {
    match sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2"
    )
    .bind(key.as_str())
    .bind(&body.value)
    .execute(pool.get_ref())
    .await
    {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": "ok"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_all_settings)
        .service(update_setting);
}

// Helper: read a setting value
pub async fn get_setting(pool: &PgPool, key: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT value FROM app_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}
