use std::sync::Arc;

use rmcp::model::{
    CallToolRequestMethod, CallToolRequestParams, CallToolResponse, CallToolResult,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};

use crate::auth::AuthContext;
use crate::state::DryRunStore;
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
    pub public_url: Option<String>,
}

impl SocMcpServer {
    fn auth(
        &self,
        context: &rmcp::service::RequestContext<RoleServer>,
    ) -> Result<AuthContext, McpError> {
        request_auth(context)
            .ok_or_else(|| McpError::invalid_params("request authentication context missing", None))
    }

    fn tool_def(&self, name: &str) -> Option<(rmcp::model::Tool, ToolClass)> {
        catalog()
            .into_iter()
            .find(|t| t.name == name)
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
            if name == "get_selected_streams" {
                self.selected_streams(&auth, &args).await
            } else {
                dispatch_read(&self.backend, &name, &args).await
            }
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
    /// Audit carries the operator-supplied url only, never the bundle.
    async fn selected_streams(
        &self,
        auth: &AuthContext,
        args: &serde_json::Value,
    ) -> Result<Value, String> {
        let target = args
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        match crate::streams::run(args, &self.backend, self.public_url.as_deref()).await {
            Ok(value) => {
                crate::audit::read_result(auth, "get_selected_streams", &target, "ok", "");
                Ok(value)
            }
            Err(message) => {
                crate::audit::read_result(
                    auth,
                    "get_selected_streams",
                    &target,
                    "rejected",
                    &message,
                );
                Err(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> SocMcpServer {
        let config = crate::config::Config {
            backend_url: "http://127.0.0.1:8000".to_string(),
            service_token: String::new(),
            mode: crate::config::Mode::ReadOnly,
            bind: "127.0.0.1:0".to_string(),
            public_url: None,
        };
        SocMcpServer {
            backend: Arc::new(crate::backend::BackendClient::new(&config).unwrap()),
            dryruns: Arc::new(DryRunStore::new()),
            mode_read_only: true,
            public_url: None,
        }
    }

    #[test]
    fn get_selected_streams_resolves_without_any_opt_in() {
        let s = server();
        let (tool, class) = s.tool_def("get_selected_streams").unwrap();
        assert_eq!(class, ToolClass::ReadOnly);
        assert_eq!(tool.name.as_ref(), "get_selected_streams");
        assert!(s.tool_def("fetch_url").is_none());
        assert!(s.tool_def("list_services").is_some());
    }
}
