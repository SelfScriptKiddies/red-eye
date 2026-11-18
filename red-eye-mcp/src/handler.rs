use std::sync::Arc;

use rmcp::model::{
    CallToolRequestMethod, CallToolRequestParams, CallToolResponse, CallToolResult,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};

use crate::auth::AuthContext;
use crate::state::{DryRunStore, RateLimiter, token_hash};
use crate::tools::{ToolClass, catalog, dispatch_mutation, dispatch_read, validate_args};

use serde_json::Value;

fn request_auth(context: &rmcp::service::RequestContext<RoleServer>) -> Option<AuthContext> {
    // rmcp forwards the HTTP request Parts into the RequestContext
    // extensions (StreamableHttpService contract); the middleware put the
    // validated identity there.
    let parts = context.extensions.get::<axum::http::request::Parts>()?;
    parts.extensions.get::<AuthContext>().cloned()
}

// The MCP server handler. Tool listing and calls both resolve the
// per-request AuthContext task-local (set by the axum middleware after
// backend validation): a read_only token never sees or reaches the
// mutation tools, and a full_control process only exposes them after the
// dual process opt-in.

#[derive(Clone)]
pub struct SocMcpServer {
    pub backend: Arc<crate::backend::BackendClient>,
    pub dryruns: Arc<DryRunStore>,
    pub mode_read_only: bool,
    pub fetch_url_enabled: bool,
    pub fetch_limit: Arc<RateLimiter>,
}

impl SocMcpServer {
    fn auth(
        &self,
        context: &rmcp::service::RequestContext<RoleServer>,
    ) -> Result<AuthContext, McpError> {
        request_auth(context)
            .ok_or_else(|| McpError::invalid_params("request authentication context missing", None))
    }

    /// Single source of the opt-in gate: a disabled fetch_url resolves to
    /// no tool at all, so both listing and dispatch fail closed as
    /// "unknown tool".
    fn tool_def(&self, name: &str) -> Option<(rmcp::model::Tool, ToolClass)> {
        catalog()
            .into_iter()
            .find(|t| t.name == name)
            .filter(|t| crate::tools::opt_in_allowed(t.class, self.fetch_url_enabled))
            .map(|t| (to_rmcp_tool(&t), t.class))
    }
}

fn to_rmcp_tool(def: &crate::tools::ToolDef) -> rmcp::model::Tool {
    let schema: serde_json::Map<String, serde_json::Value> =
        def.schema.as_object().cloned().unwrap_or_default();
    rmcp::model::Tool::new(def.name, def.description, schema)
}

impl ServerHandler for SocMcpServer {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new(
                "red-eye-mcp",
                env!("CARGO_PKG_VERSION"),
            ));
        info.instructions = Some(if self.mode_read_only {
            "Bounded read-only Red Eye inspection: lists, stream summaries, bounded packet and message evidence. No mutations exist in this process.".to_string()
        } else {
            "Red Eye inspection and FULL CONTROL. Mutations require a full_control token, an expected revision, confirm=true, an audit reason, and for destructive operations a prior dry-run operation id.".to_string()
        });
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let auth = self.auth(&context)?;
        let tools: Vec<rmcp::model::Tool> = catalog()
            .iter()
            .filter(|t| match t.class {
                ToolClass::ReadOnly => true,
                ToolClass::Mutation => auth.capability == "full_control" && !self.mode_read_only,
                ToolClass::Fetch => self.fetch_url_enabled,
            })
            .map(to_rmcp_tool)
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let auth = self.auth(&context)?;
        let name: String = request.name.to_string();
        let Some((_, class)) = self.tool_def(&name) else {
            return Err(McpError::method_not_found::<CallToolRequestMethod>());
        };
        // Defense in depth on top of the middleware scope check: the
        // token capability decides again at dispatch time.
        if class == ToolClass::Mutation
            && (auth.capability != "full_control" || self.mode_read_only)
        {
            crate::audit::mutation_denied(&auth, &name, "capability");
            return Err(McpError::invalid_params(
                "tool requires a full_control token in a full_control process",
                None,
            ));
        }
        let args = validate_args(request.arguments.as_ref())
            .map_err(|m| McpError::invalid_params(m, None))?;

        let result = if class == ToolClass::ReadOnly {
            dispatch_read(&self.backend, &name, &args).await
        } else if class == ToolClass::Fetch {
            self.fetch(&auth, &args).await
        } else {
            crate::audit::mutation_attempt(&auth, &name, &args);
            dispatch_mutation(&self.backend, &self.dryruns, &name, &args).await
        };

        match result {
            Ok(value) => {
                if class == ToolClass::Mutation {
                    crate::audit::mutation_result(&auth, &name, "ok", "");
                }
                // Structured JSON plus the same JSON as text: bounded
                // clients can read either.
                let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
                Ok(rmcp::model::CallToolResponse::Complete(
                    CallToolResult::success(vec![rmcp::model::ContentBlock::text(text)]),
                ))
            }
            Err(message) => {
                if class == ToolClass::Mutation {
                    crate::audit::mutation_result(&auth, &name, "rejected", &message);
                }
                Err(McpError::invalid_params(message, None))
            }
        }
    }
}

impl SocMcpServer {
    /// Any authenticated token (read_only or full_control) may fetch:
    /// the tool mutates nothing in Red Eye. Budgeted by the fetch-specific
    /// per-token limiter on top of the middleware's per-token and
    /// per-address request limits.
    async fn fetch(&self, auth: &AuthContext, args: &serde_json::Value) -> Result<Value, String> {
        let key = token_hash(&auth.token_id.to_string());
        match self.fetch_limit.check(&key) {
            Some(true) => {}
            Some(false) => {
                crate::audit::fetch_result(auth, "fetch_url", "", "rate-limited", 0, "");
                return Err("fetch rate limit exceeded for token".to_string());
            }
            None => return Err("fetch rate limiter unavailable".to_string()),
        }
        let requested = args
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .chars()
            .take(crate::config::MAX_URL_LEN)
            .collect::<String>();
        match crate::fetch::run(args).await {
            Ok(value) => {
                let target = value["final_url"].as_str().unwrap_or(&requested);
                let bytes = value["bytes"].as_u64().unwrap_or(0);
                crate::audit::fetch_result(auth, "fetch_url", target, "ok", bytes, "");
                Ok(value)
            }
            Err(e) => {
                crate::audit::fetch_result(
                    auth,
                    "fetch_url",
                    &requested,
                    "rejected",
                    0,
                    &e.to_string(),
                );
                Err(e.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(fetch_enabled: bool) -> SocMcpServer {
        let config = crate::config::Config {
            backend_url: "http://127.0.0.1:8000".to_string(),
            service_token: String::new(),
            mode: crate::config::Mode::ReadOnly,
            bind: "127.0.0.1:0".to_string(),
            fetch_url_enabled: fetch_enabled,
        };
        SocMcpServer {
            backend: Arc::new(crate::backend::BackendClient::new(&config).unwrap()),
            dryruns: Arc::new(DryRunStore::new()),
            mode_read_only: true,
            fetch_url_enabled: fetch_enabled,
            fetch_limit: Arc::new(RateLimiter::fetch()),
        }
    }

    #[test]
    fn fetch_url_is_unknown_when_disabled() {
        let off = server(false);
        // Fail closed: a disabled opt-in tool resolves to no tool at all,
        // the same path as a genuinely unknown tool name.
        assert!(off.tool_def("fetch_url").is_none());
        assert!(off.tool_def("list_services").is_some());
        let on = server(true);
        let (tool, class) = on.tool_def("fetch_url").unwrap();
        assert_eq!(class, ToolClass::Fetch);
        assert_eq!(tool.name.as_ref(), "fetch_url");
    }

    #[test]
    fn fetch_budget_is_tighter_than_the_global_limit() {
        let limiter = RateLimiter::fetch();
        let budget = crate::config::FETCH_RATE_LIMIT_PER_TOKEN;
        for _ in 0..budget {
            assert_eq!(limiter.check("t"), Some(true));
        }
        assert_eq!(limiter.check("t"), Some(false));
    }
}
