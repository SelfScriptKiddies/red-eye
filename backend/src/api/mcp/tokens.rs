use actix_identity::Identity;
use actix_web::{HttpRequest, HttpResponse, Responder, delete, get, post, put, web};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

use crate::api::soc::bearer;

// MCP harness token management + the authorization endpoint red-eye-mcp
// calls on every incoming request. Token CRUD is operator-only (session
// cookie) and is NEVER itself an MCP tool; authz requires the MCP
// service credential. Only SHA-256 hashes are stored or listed; the
// plaintext exists solely in the create response.

const MAX_TOKENS: i64 = 100;
const MAX_EXPIRY_SECS: i64 = 365 * 86_400;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct TokenCreate {
    pub name: String,
    /// read_only | full_control
    pub capability: String,
    /// Optional lifetime in seconds; absent = never expires.
    pub expires_in_secs: Option<i64>,
}

#[get("/mcp/tokens")]
async fn list_tokens(_identity: Identity, pool: web::Data<PgPool>) -> impl Responder {
    match sqlx::query_as::<_, (i64, String, String, Option<f64>, Option<f64>)>(
        r#"
        SELECT id, name, capability,
               EXTRACT(EPOCH FROM expires_at)::float8 AS expires_at,
               EXTRACT(EPOCH FROM revoked_at)::float8 AS revoked_at
        FROM mcp_token
        ORDER BY id DESC
        LIMIT $1
        "#,
    )
    .bind(MAX_TOKENS)
    .fetch_all(pool.get_ref())
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|(id, name, capability, expires_at, revoked_at)| {
                    json!({
                        "id": id, "name": name, "capability": capability,
                        "expires_at_unix": expires_at, "revoked": revoked_at.is_some(),
                    })
                })
                .collect::<Vec<_>>(),
        ),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[post("/mcp/tokens")]
async fn create_token(
    _identity: Identity,
    pool: web::Data<PgPool>,
    body: web::Json<TokenCreate>,
) -> impl Responder {
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 128 {
        return HttpResponse::BadRequest().json(json!({"error": "name must be 1-128 characters"}));
    }
    let capability = body.capability.trim();
    if capability != "read_only" && capability != "full_control" {
        return HttpResponse::BadRequest()
            .json(json!({"error": "capability must be read_only or full_control"}));
    }
    if capability == "full_control" {
        let svc = crate::api::settings::get_setting(pool.get_ref(), "mcp_service_capability")
            .await
            .unwrap_or_else(|| "read_only".to_string());
        if svc != "full_control" {
            return HttpResponse::Conflict().json(json!({
                "error": "full_control tokens require mcp_service_capability=full_control \
                          and red-eye-mcp deployed with MCP_MODE=full_control",
            }));
        }
    }
    let expires_at: Option<f64> = body
        .expires_in_secs
        .filter(|s| *s > 0 && *s <= MAX_EXPIRY_SECS)
        .map(|s| unix_now() + s as f64);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_token")
        .fetch_one(pool.get_ref())
        .await
        .unwrap_or(0);
    if count >= MAX_TOKENS {
        return HttpResponse::Conflict()
            .json(json!({"error": "token limit reached; revoke unused tokens first"}));
    }

    let plaintext = match bearer::generate_token() {
        Ok(t) => t,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(json!({"error": format!("token generation failed: {}", e)}));
        }
    };
    let hash = bearer::token_hash(&plaintext);
    match sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mcp_token (name, token_hash, capability, expires_at)
        VALUES ($1, $2, $3, CASE WHEN $4::float8 IS NULL THEN NULL ELSE to_timestamp($4::float8) END)
        RETURNING id
        "#,
    )
    .bind(name)
    .bind(&hash)
    .bind(capability)
    .bind(expires_at)
    .fetch_one(pool.get_ref())
    .await
    {
        Ok(id) => HttpResponse::Created().json(json!({
            "id": id,
            "token": plaintext,
            "capability": capability,
            "note": "store this token now; it is never shown again",
        })),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[delete("/mcp/tokens/{id}")]
async fn revoke_token(
    _identity: Identity,
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
) -> impl Responder {
    match sqlx::query(
        "UPDATE mcp_token SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(path.into_inner())
    .execute(pool.get_ref())
    .await
    {
        Ok(r) if r.rows_affected() > 0 => HttpResponse::Ok().json(json!({"status": "revoked"})),
        Ok(_) => {
            HttpResponse::NotFound().json(json!({"error": "token not found or already revoked"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct AuthzRequest {
    pub token: String,
}

/// POST /api/mcp/tokens/authz: validate one presented MCP bearer token.
/// Hash lookup, expiry and revocation checks; unknown/expired/revoked
/// all collapse into 401 so the caller cannot distinguish states.
/// last_used_at updates are throttled by the caller's request volume;
/// here they are best-effort and skipped entirely on older-than-a-minute
/// stamps.
#[post("/mcp/tokens/authz")]
async fn authz(
    pool: web::Data<PgPool>,
    req: HttpRequest,
    body: web::Json<AuthzRequest>,
) -> impl Responder {
    // The MCP service credential guards this endpoint: an attacker with
    // network access must not be able to brute-force harness tokens.
    if let Err(resp) = bearer::require_service_token(
        pool.get_ref(),
        &req,
        crate::api::soc::settings::MCP_SERVICE_TOKEN,
        Some("mcp_service_last_seen"),
    )
    .await
    {
        return resp;
    }
    let hash = bearer::token_hash(body.token.trim());
    match sqlx::query_as::<_, (i64, String, String, Option<f64>, Option<f64>, Option<f64>)>(
        r#"
        SELECT id, name, capability,
               EXTRACT(EPOCH FROM expires_at)::float8,
               EXTRACT(EPOCH FROM revoked_at)::float8,
               EXTRACT(EPOCH FROM last_used_at)::float8
        FROM mcp_token WHERE token_hash = $1
        "#,
    )
    .bind(&hash)
    .fetch_optional(pool.get_ref())
    .await
    {
        Ok(Some((id, name, capability, expires_at, revoked_at, last_used))) => {
            let now = unix_now();
            let expired = expires_at.map(|e| e < now).unwrap_or(false);
            if revoked_at.is_some() || expired {
                return HttpResponse::Unauthorized().json(json!({"error": "token not valid"}));
            }
            if last_used.map(|t| now - t > 60.0).unwrap_or(true) {
                let _ = sqlx::query("UPDATE mcp_token SET last_used_at = now() WHERE id = $1")
                    .bind(id)
                    .execute(pool.get_ref())
                    .await;
            }
            HttpResponse::Ok().json(json!({
                "token_id": id,
                "name": name,
                "capability": capability,
            }))
        }
        Ok(None) => HttpResponse::Unauthorized().json(json!({"error": "token not valid"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// PUT /api/mcp/tokens/{id}/capability: server-side scope change. Takes
/// effect on the next authz call; a read_only -> full_control flip is a
/// deliberate operator action that the MCP settings UI flags.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct CapabilityUpdate {
    pub capability: String,
}

#[put("/mcp/tokens/{id}/capability")]
async fn set_capability(
    _identity: Identity,
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
    body: web::Json<CapabilityUpdate>,
) -> impl Responder {
    let capability = body.capability.trim();
    if capability != "read_only" && capability != "full_control" {
        return HttpResponse::BadRequest()
            .json(json!({"error": "capability must be read_only or full_control"}));
    }
    match sqlx::query("UPDATE mcp_token SET capability = $2 WHERE id = $1 AND revoked_at IS NULL")
        .bind(path.into_inner())
        .bind(capability)
        .execute(pool.get_ref())
        .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            HttpResponse::Ok().json(json!({"capability": capability}))
        }
        Ok(_) => HttpResponse::NotFound().json(json!({"error": "token not found or revoked"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(list_tokens)
        .service(create_token)
        .service(revoke_token)
        .service(authz)
        .service(set_capability);
}

#[cfg(test)]
mod tests {
    #[test]
    fn hash_lookup_cannot_match_plaintext_storage() {
        // The stored form is the SHA-256 hex digest, never the token.
        let token = "abcdef123456";
        let hash = crate::api::soc::bearer::token_hash(token);
        assert_ne!(hash, token);
        assert_eq!(hash.len(), 64);
    }
}
