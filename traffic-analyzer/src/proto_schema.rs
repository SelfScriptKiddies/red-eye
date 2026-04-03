use std::collections::HashMap;
use log::debug;

/// Parsed .proto schema — maps gRPC service/method to field definitions.
#[derive(Debug, Clone)]
pub struct ProtoSchema {
    /// package name (e.g., "ctf")
    pub package: String,
    /// service_name → method_name → (request_type, response_type)
    pub methods: HashMap<String, HashMap<String, (String, String)>>,
    /// message_name → field_number → field_name
    pub messages: HashMap<String, HashMap<u64, String>>,
}

impl ProtoSchema {
    /// Resolve field names for a gRPC call.
    /// `service_path` is the full gRPC service name (e.g., "ctf.UserService")
    /// Returns field_number → field_name map.
    pub fn resolve_fields(
        &self,
        service_path: &str,
        method_name: &str,
        is_request: bool,
    ) -> Option<&HashMap<u64, String>> {
        // service_path can be "ctf.UserService" or just "UserService"
        let svc_name = service_path
            .rsplit('.')
            .next()
            .unwrap_or(service_path);

        let method_map = self.methods.get(svc_name)?;
        let (req_type, resp_type) = method_map.get(method_name)?;
        let msg_type = if is_request { req_type } else { resp_type };
        self.messages.get(msg_type.as_str())
    }
}

/// Parse a .proto file into a ProtoSchema.
/// Handles proto3 syntax with basic message/service/rpc definitions.
pub fn parse_proto(content: &str) -> ProtoSchema {
    let mut package = String::new();
    let mut methods: HashMap<String, HashMap<String, (String, String)>> = HashMap::new();
    let mut messages: HashMap<String, HashMap<u64, String>> = HashMap::new();

    // Remove comments
    let content = remove_comments(content);
    let mut chars = content.chars().peekable();
    let mut tokens = tokenize(&content);
    let mut pos = 0;

    while pos < tokens.len() {
        match tokens[pos].as_str() {
            "package" => {
                pos += 1;
                if pos < tokens.len() {
                    package = tokens[pos].trim_end_matches(';').to_string();
                    pos += 1;
                }
            }
            "service" => {
                pos += 1;
                if pos < tokens.len() {
                    let svc_name = tokens[pos].clone();
                    pos += 1;
                    let svc_methods = parse_service_block(&tokens, &mut pos);
                    methods.insert(svc_name, svc_methods);
                }
            }
            "message" => {
                pos += 1;
                if pos < tokens.len() {
                    let msg_name = tokens[pos].clone();
                    pos += 1;
                    let fields = parse_message_block(&tokens, &mut pos);
                    messages.insert(msg_name, fields);
                }
            }
            _ => {
                pos += 1;
            }
        }
    }

    debug!(
        "Parsed proto: package={}, services={}, messages={}",
        package,
        methods.len(),
        messages.len()
    );

    ProtoSchema {
        package,
        methods,
        messages,
    }
}

/// Merge multiple proto files (for the same service) into one schema.
pub fn merge_protos(protos: &[ProtoSchema]) -> ProtoSchema {
    let mut merged = ProtoSchema {
        package: String::new(),
        methods: HashMap::new(),
        messages: HashMap::new(),
    };
    for p in protos {
        if !p.package.is_empty() {
            merged.package = p.package.clone();
        }
        for (svc, methods) in &p.methods {
            merged
                .methods
                .entry(svc.clone())
                .or_default()
                .extend(methods.clone());
        }
        merged.messages.extend(p.messages.clone());
    }
    merged
}

fn remove_comments(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let bytes = content.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
                result.push('\n');
            }
            i += 1;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

fn tokenize(content: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in content.chars() {
        match ch {
            '{' | '}' | '(' | ')' | ';' | '=' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push(ch.to_string());
            }
            ' ' | '\t' | '\n' | '\r' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Parse `{ rpc Name(ReqType) returns (RespType); ... }`
fn parse_service_block(
    tokens: &[String],
    pos: &mut usize,
) -> HashMap<String, (String, String)> {
    let mut methods = HashMap::new();
    // Skip to opening brace
    while *pos < tokens.len() && tokens[*pos] != "{" {
        *pos += 1;
    }
    *pos += 1; // skip {
    let mut depth = 1;

    while *pos < tokens.len() && depth > 0 {
        if tokens[*pos] == "{" {
            depth += 1;
            *pos += 1;
            continue;
        }
        if tokens[*pos] == "}" {
            depth -= 1;
            *pos += 1;
            continue;
        }
        if tokens[*pos] == "rpc" {
            *pos += 1;
            if *pos + 6 < tokens.len() {
                // rpc Name ( ReqType ) returns ( RespType )
                let method_name = tokens[*pos].clone();
                *pos += 1; // (
                *pos += 1; // skip (
                let req_type = tokens[*pos].clone();
                *pos += 1; // skip req_type
                *pos += 1; // skip )
                // "returns"
                if *pos < tokens.len() && tokens[*pos] == "returns" {
                    *pos += 1;
                }
                *pos += 1; // skip (
                if *pos < tokens.len() {
                    let resp_type = tokens[*pos].clone();
                    methods.insert(method_name, (req_type, resp_type));
                }
                *pos += 1; // skip resp_type
                *pos += 1; // skip )
            }
        } else {
            *pos += 1;
        }
    }
    methods
}

/// Parse `{ type name = number; ... }` — extracts field_number → field_name
fn parse_message_block(
    tokens: &[String],
    pos: &mut usize,
) -> HashMap<u64, String> {
    let mut fields = HashMap::new();
    while *pos < tokens.len() && tokens[*pos] != "{" {
        *pos += 1;
    }
    *pos += 1; // skip {
    let mut depth = 1;

    while *pos < tokens.len() && depth > 0 {
        if tokens[*pos] == "{" {
            depth += 1;
            *pos += 1;
            continue;
        }
        if tokens[*pos] == "}" {
            depth -= 1;
            *pos += 1;
            continue;
        }
        // Skip nested messages/enums/oneof
        if tokens[*pos] == "message" || tokens[*pos] == "enum" || tokens[*pos] == "oneof" {
            *pos += 1; // skip name
            *pos += 1;
            // Skip to matching brace
            while *pos < tokens.len() && tokens[*pos] != "{" {
                *pos += 1;
            }
            *pos += 1; // skip {
            let mut inner_depth = 1;
            while *pos < tokens.len() && inner_depth > 0 {
                if tokens[*pos] == "{" { inner_depth += 1; }
                if tokens[*pos] == "}" { inner_depth -= 1; }
                *pos += 1;
            }
            continue;
        }
        // Skip keywords
        if ["reserved", "option", "extensions", "map"].contains(&tokens[*pos].as_str()) {
            while *pos < tokens.len() && tokens[*pos] != ";" {
                *pos += 1;
            }
            *pos += 1; // skip ;
            continue;
        }

        // Try to parse: [optional|repeated] type name = number ;
        let start = *pos;
        // Skip optional/repeated/required
        if ["optional", "repeated", "required"].contains(&tokens[*pos].as_str()) {
            *pos += 1;
        }
        // type
        if *pos < tokens.len() {
            *pos += 1; // skip type
        }
        // name
        if *pos < tokens.len() && *pos + 2 < tokens.len() && tokens[*pos + 1] == "=" {
            let field_name = tokens[*pos].clone();
            *pos += 1; // skip name
            *pos += 1; // skip =
            if *pos < tokens.len() {
                if let Ok(num) = tokens[*pos].trim_end_matches(';').parse::<u64>() {
                    fields.insert(num, field_name);
                }
                *pos += 1; // skip number
            }
            // Skip to semicolon
            while *pos < tokens.len() && tokens[*pos] != ";" {
                *pos += 1;
            }
            if *pos < tokens.len() {
                *pos += 1; // skip ;
            }
        } else {
            // Not a field definition, skip to semicolon or next token
            *pos = start + 1;
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_proto() {
        let content = r#"
            syntax = "proto3";
            package ctf;

            service UserService {
              rpc Login (LoginRequest) returns (LoginResponse);
              rpc GetUser (GetUserRequest) returns (GetUserResponse);
            }

            message LoginRequest {
              string username = 1;
              string password = 2;
            }

            message LoginResponse {
              string token = 1;
              bool success = 2;
            }

            message GetUserRequest {
              int32 user_id = 1;
            }

            message GetUserResponse {
              int32 user_id = 1;
              string username = 2;
              string email = 3;
              string role = 4;
            }
        "#;

        let schema = parse_proto(content);
        assert_eq!(schema.package, "ctf");
        assert_eq!(schema.methods.len(), 1);
        assert_eq!(schema.messages.len(), 4);

        // Check method mapping
        let user_svc = schema.methods.get("UserService").unwrap();
        let (req, resp) = user_svc.get("Login").unwrap();
        assert_eq!(req, "LoginRequest");
        assert_eq!(resp, "LoginResponse");

        // Check field mapping
        let login_req = schema.messages.get("LoginRequest").unwrap();
        assert_eq!(login_req.get(&1), Some(&"username".to_string()));
        assert_eq!(login_req.get(&2), Some(&"password".to_string()));

        // Check resolve
        let fields = schema.resolve_fields("ctf.UserService", "Login", true).unwrap();
        assert_eq!(fields.get(&1), Some(&"username".to_string()));
        assert_eq!(fields.get(&2), Some(&"password".to_string()));

        let fields = schema.resolve_fields("ctf.UserService", "Login", false).unwrap();
        assert_eq!(fields.get(&1), Some(&"token".to_string()));
        assert_eq!(fields.get(&2), Some(&"success".to_string()));

        let fields = schema.resolve_fields("ctf.UserService", "GetUser", false).unwrap();
        assert_eq!(fields.get(&1), Some(&"user_id".to_string()));
        assert_eq!(fields.get(&4), Some(&"role".to_string()));
    }
}
