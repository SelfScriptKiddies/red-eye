use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, get, put, web};
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::api::ai::settings::AI_API_KEY;
use crate::api::soc::settings::{SOC_ENABLED, mask_soc_secrets_in_map};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SettingUpdate {
    pub value: String,
}

/// GET-side masking: the JEV API key is write-only, so every settings map
/// leaving the backend replaces it with a bare configured flag. No masked
/// value, no echo. Extracted for tests: the GET handler and any future
/// settings-exporting endpoint must go through this.
pub fn mask_ai_key_in_map(map: &mut HashMap<String, String>) {
    if let Some(key) = map.remove(AI_API_KEY) {
        map.insert(
            "ai_api_key_configured".to_string(),
            (!key.trim().is_empty()).to_string(),
        );
    } else {
        map.insert("ai_api_key_configured".to_string(), "false".to_string());
    }
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
            mask_ai_key_in_map(&mut map);
            mask_soc_secrets_in_map(&mut map);
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
    // Favorite capacity keys are byte budgets: reject non-positive or
    // non-numeric values instead of silently breaking the star admission.
    if key.starts_with("favorite_") {
        let parsed = body.value.trim().parse::<i64>().ok().filter(|v| *v > 0);
        if parsed.is_none() {
            return HttpResponse::BadRequest().json(serde_json::json!(
                {"error": "favorite settings are byte budgets and must be positive integers"}
            ));
        }
    }
    // JEV settings are re-read by the scheduler every tick: a bad value
    // must never reach it. Each key has its own typed bound; the previous
    // value stays untouched on rejection.
    if key.starts_with("ai_") {
        if let Err(msg) =
            crate::api::ai::validate_setting(key.as_str(), &body.value, pool.get_ref()).await
        {
            return HttpResponse::BadRequest().json(serde_json::json!({"error": msg}));
        }
    }
    // SOC settings share the contract: typed bounds, previous value kept
    // on rejection, generated credentials read-only. Disabling SOC is
    // significant state: the enablement generation advances on every
    // false->true transition.
    if key.starts_with("soc_") || key.starts_with("mcp_") {
        if let Err(msg) =
            crate::api::soc::settings::validate_setting(key.as_str(), &body.value, pool.get_ref())
                .await
        {
            return HttpResponse::BadRequest().json(serde_json::json!({"error": msg}));
        }
    }
    // soc_enabled false->true bumps the settings generation so late
    // results queued before a disable are rejected after re-enable.
    if key.as_str() == SOC_ENABLED && body.value.trim().eq_ignore_ascii_case("true") {
        let was = crate::api::settings::get_setting(pool.get_ref(), SOC_ENABLED)
            .await
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !was {
            crate::api::soc::advance_generation(pool.get_ref()).await;
        }
    }
    match sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2",
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
    cfg.service(get_all_settings).service(update_setting);
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
