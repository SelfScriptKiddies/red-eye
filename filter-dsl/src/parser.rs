//! Recursive-descent parser with static type checking.
//!
//! Grammar (loosest binding first):
//!
//! ```text
//! expr     := or
//! or       := and ("||" and)*
//! and      := unary ("&&" unary)*
//! unary    := "!" unary | cmp
//! cmp      := postfix (op postfix)?          op in == != < <= > >=
//! postfix  := primary ("." ident "(" args ")")*
//! primary  := "(" expr ")" | literal | field | call
//! literal  := uint | "str" | b"bytes" | true | false
//! call     := ident "(" args ")"             only ip_in_cidr
//! ```

use crate::lexer::{self, Token};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Bool,
    Uint,
    Str,
    Bytes,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Bool => write!(f, "bool"),
            Type::Uint => write!(f, "uint"),
            Type::Str => write!(f, "string"),
            Type::Bytes => write!(f, "bytes"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Bool(bool),
    Uint(u64),
    Str(String),
    Bytes(Vec<u8>),
    Field(FieldId),
    Not(Box<Node>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    /// Both operands share the comparison's node type.
    Cmp(CmpOp, Box<Node>, Box<Node>),
    /// ip_in_cidr(ip, cidr); args are Str-typed nodes.
    IpInCidr(Box<Node>, Box<Node>),
    /// contains: Str.contains(Str) or Bytes.contains(Bytes).
    Contains(Box<Node>, Box<Node>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldId {
    Payload,
    SrcIp,
    DstIp,
    SrcPort,
    DstPort,
    LocalIp,
    RemoteIp,
    LocalPort,
    RemotePort,
    Protocol,
    Direction,
    ServiceId,
    AgentId,
    Timestamp,
}

fn field_type(id: FieldId) -> Type {
    match id {
        FieldId::Payload => Type::Bytes,
        FieldId::SrcIp
        | FieldId::DstIp
        | FieldId::LocalIp
        | FieldId::RemoteIp
        | FieldId::Protocol
        | FieldId::Direction
        | FieldId::AgentId => Type::Str,
        FieldId::SrcPort
        | FieldId::DstPort
        | FieldId::LocalPort
        | FieldId::RemotePort
        | FieldId::ServiceId
        | FieldId::Timestamp => Type::Uint,
    }
}

fn field_by_name(name: &str) -> Option<FieldId> {
    use FieldId::*;
    Some(match name {
        "payload" => Payload,
        "src_ip" => SrcIp,
        "dst_ip" => DstIp,
        "src_port" => SrcPort,
        "dst_port" => DstPort,
        "local_ip" => LocalIp,
        "remote_ip" => RemoteIp,
        "local_port" => LocalPort,
        "remote_port" => RemotePort,
        "protocol" => Protocol,
        "direction" => Direction,
        "service_id" => ServiceId,
        "agent_id" => AgentId,
        "timestamp" => Timestamp,
        _ => return None,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    pub root: Node,
    pub ty: Type,
}

#[derive(Debug)]
pub enum CompileError {
    SourceTooLong(usize),
    TooDeep,
    Syntax(String),
    Type(String),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::SourceTooLong(max) => {
                write!(f, "expression source exceeds the {} character limit", max)
            }
            CompileError::TooDeep => write!(f, "expression nesting exceeds the depth limit"),
            CompileError::Syntax(msg) => write!(f, "syntax error: {}", msg),
            CompileError::Type(msg) => write!(f, "type error: {}", msg),
        }
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
}

pub fn compile(source: &str) -> Result<Expr, CompileError> {
    if source.len() > crate::MAX_SOURCE_LEN {
        return Err(CompileError::SourceTooLong(crate::MAX_SOURCE_LEN));
    }
    let tokens = lexer::tokenize(source).map_err(|e| CompileError::Syntax(e.to_string()))?;
    let mut parser = Parser {
        tokens,
        pos: 0,
        depth: 0,
    };
    let root = parser.parse_expr()?;
    let (ty, root) = type_check(root)?;
    if ty != Type::Bool {
        return Err(CompileError::Type(format!(
            "expression must return bool, found {}",
            ty
        )));
    }
    if !matches!(parser.peek(), Token::Eof) {
        return Err(CompileError::Syntax(format!(
            "unexpected trailing {}",
            parser.peek()
        )));
    }
    Ok(Expr { root, ty })
}

/// Bottom-up static type check: every node's type is derived from its
/// children, so misuse (string + uint comparison, contains on a bool, ...)
/// is rejected before persistence and never reached at evaluation time.
fn type_check(node: Node) -> Result<(Type, Node), CompileError> {
    Ok(match node {
        Node::Bool(_) => (Type::Bool, node),
        Node::Uint(_) => (Type::Uint, node),
        Node::Str(_) => (Type::Str, node),
        Node::Bytes(_) => (Type::Bytes, node),
        Node::Field(id) => (field_type(id), node),
        Node::Not(inner) => {
            let (ty, inner) = type_check(*inner)?;
            if ty != Type::Bool {
                return Err(CompileError::Type(format!(
                    "'!' requires bool, found {}",
                    ty
                )));
            }
            (Type::Bool, Node::Not(Box::new(inner)))
        }
        Node::And(..) | Node::Or(..) => {
            let is_and = matches!(node, Node::And(..));
            let (a, b) = match node {
                Node::And(a, b) | Node::Or(a, b) => (*a, *b),
                _ => unreachable!(),
            };
            let (ta, a) = type_check(a)?;
            let (tb, b) = type_check(b)?;
            if ta != Type::Bool || tb != Type::Bool {
                return Err(CompileError::Type(format!(
                    "'{}' requires bool operands, found {} and {}",
                    if is_and { "&&" } else { "||" },
                    ta,
                    tb
                )));
            }
            let combined = if is_and {
                Node::And(Box::new(a), Box::new(b))
            } else {
                Node::Or(Box::new(a), Box::new(b))
            };
            (Type::Bool, combined)
        }
        Node::Cmp(op, a, b) => {
            let (ta, a) = type_check(*a)?;
            let (tb, b) = type_check(*b)?;
            if ta != tb {
                return Err(CompileError::Type(format!(
                    "comparison mixes {} and {}",
                    ta, tb
                )));
            }
            match ta {
                Type::Bool => {
                    if !matches!(op, CmpOp::Eq | CmpOp::Neq) {
                        return Err(CompileError::Type(
                            "bools support only == and !=".to_string(),
                        ));
                    }
                }
                Type::Uint | Type::Str | Type::Bytes => {}
            }
            (Type::Bool, Node::Cmp(op, Box::new(a), Box::new(b)))
        }
        Node::IpInCidr(a, b) => {
            let (ta, a) = type_check(*a)?;
            let (tb, b) = type_check(*b)?;
            if ta != Type::Str || tb != Type::Str {
                return Err(CompileError::Type(
                    "ip_in_cidr requires two string arguments".to_string(),
                ));
            }
            // A literal cidr is validated now: a typo must fail at save
            // time, not hide as a permanent evaluation error later.
            if let Node::Str(cidr) = &b {
                if crate::ip::parse_cidr(cidr).is_none() {
                    return Err(CompileError::Type(format!(
                        "ip_in_cidr argument '{}' is not a valid IP or CIDR",
                        cidr
                    )));
                }
            }
            (Type::Bool, Node::IpInCidr(Box::new(a), Box::new(b)))
        }
        Node::Contains(hay, needle) => {
            let (th, hay) = type_check(*hay)?;
            let (tn, needle) = type_check(*needle)?;
            if th != tn || !matches!(th, Type::Str | Type::Bytes) {
                return Err(CompileError::Type(format!(
                    "contains requires matching string or bytes operands, found {} and {}",
                    th, tn
                )));
            }
            (Type::Bool, Node::Contains(Box::new(hay), Box::new(needle)))
        }
    })
}

impl Parser {
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn next(&mut self) -> Token {
        let tok = self.peek().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), CompileError> {
        if self.peek() == expected {
            self.pos += 1;
            Ok(())
        } else {
            Err(CompileError::Syntax(format!(
                "expected {}, found {}",
                expected,
                self.peek()
            )))
        }
    }

    fn enter(&mut self) -> Result<(), CompileError> {
        self.depth += 1;
        if self.depth > crate::MAX_AST_DEPTH {
            Err(CompileError::TooDeep)
        } else {
            Ok(())
        }
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn parse_expr(&mut self) -> Result<Node, CompileError> {
        self.enter()?;
        let mut node = self.parse_and()?;
        while matches!(self.peek(), Token::OrOr) {
            self.next();
            let rhs = self.parse_and()?;
            node = Node::Or(Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_and(&mut self) -> Result<Node, CompileError> {
        self.enter()?;
        let mut node = self.parse_unary()?;
        while matches!(self.peek(), Token::AndAnd) {
            self.next();
            let rhs = self.parse_unary()?;
            node = Node::And(Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_unary(&mut self) -> Result<Node, CompileError> {
        self.enter()?;
        let node = if matches!(self.peek(), Token::Not) {
            self.next();
            Ok(Node::Not(Box::new(self.parse_unary()?)))
        } else {
            self.parse_cmp()
        };
        self.leave();
        node
    }

    fn parse_cmp(&mut self) -> Result<Node, CompileError> {
        let lhs = self.parse_postfix()?;
        let op = match self.peek() {
            Token::Eq => Some(CmpOp::Eq),
            Token::Neq => Some(CmpOp::Neq),
            Token::Lt => Some(CmpOp::Lt),
            Token::Lte => Some(CmpOp::Lte),
            Token::Gt => Some(CmpOp::Gt),
            Token::Gte => Some(CmpOp::Gte),
            _ => None,
        };
        if let Some(op) = op {
            self.next();
            let rhs = self.parse_postfix()?;
            return Ok(Node::Cmp(op, Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_postfix(&mut self) -> Result<Node, CompileError> {
        let mut node = self.parse_primary()?;
        while matches!(self.peek(), Token::Dot) {
            self.next();
            let method = match self.next() {
                Token::Ident(name) => name,
                other => {
                    return Err(CompileError::Syntax(format!(
                        "expected a method name after '.', found {}",
                        other
                    )))
                }
            };
            self.expect(&Token::LParen)?;
            match method.as_str() {
                "contains" => {
                    let arg = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    node = Node::Contains(Box::new(node), Box::new(arg));
                }
                other => {
                    return Err(CompileError::Syntax(format!(
                        "unknown method '{}()' (only contains is available)",
                        other
                    )));
                }
            }
        }
        Ok(node)
    }

    fn parse_primary(&mut self) -> Result<Node, CompileError> {
        self.enter()?;
        let node = match self.next() {
            Token::LParen => {
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                inner
            }
            Token::Uint(n) => Node::Uint(n),
            Token::Bytes(raw) => Node::Bytes(raw),
            Token::Str(raw) => Node::Str(
                lexer::str_token_to_string(&raw, self.pos)
                    .map_err(|e| CompileError::Syntax(e.to_string()))?,
            ),
            Token::Ident(name) => match name.as_str() {
                "true" => Node::Bool(true),
                "false" => Node::Bool(false),
                "ip_in_cidr" => {
                    self.expect(&Token::LParen)?;
                    let ip = self.parse_expr()?;
                    self.expect(&Token::Comma)?;
                    let cidr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Node::IpInCidr(Box::new(ip), Box::new(cidr))
                }
                other => {
                    if let Some(field) = field_by_name(other) {
                        Node::Field(field)
                    } else {
                        return Err(CompileError::Syntax(format!(
                            "unknown identifier '{}' (fields and ip_in_cidr only)",
                            other
                        )));
                    }
                }
            },
            other => {
                return Err(CompileError::Syntax(format!(
                    "unexpected {} where a value was expected",
                    other
                )));
            }
        };
        self.leave();
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(src: &str) {
        match compile(src) {
            Ok(e) => {
                assert_eq!(e.ty, Type::Bool, "'{}' must type-check to bool", src);
            }
            Err(e) => panic!("'{}' failed to compile: {}", src, e),
        }
    }

    fn err(src: &str) -> CompileError {
        compile(src).expect_err("expected compile failure")
    }

    #[test]
    fn boolean_expressions_compile() {
        ok("true");
        ok("!false");
        ok("protocol == \"TCP\" && direction == \"in\"");
        ok("protocol == \"TCP\" || protocol == \"UDP\"");
        ok("!(src_port == 80) && dst_port >= 1000");
        ok("src_port < dst_port");
        ok("(src_port == 1 || src_port == 2) && dst_port != 3");
        ok("payload.contains(b\"token\")");
        ok("src_ip.contains(\"10.\")");
        ok("ip_in_cidr(remote_ip, \"10.0.0.0/8\")");
        ok("agent_id == \"agent-a\" && service_id == 5 && timestamp > 1000");
    }

    #[test]
    fn type_errors_are_rejected() {
        assert!(matches!(err("src_port == \"80\""), CompileError::Type(_)));
        assert!(matches!(err("!src_port"), CompileError::Type(_)));
        assert!(matches!(err("src_port && true"), CompileError::Type(_)));
        assert!(matches!(
            err("payload.contains(\"txt\")"),
            CompileError::Type(_)
        ));
        assert!(matches!(err("true < false"), CompileError::Type(_)));
        assert!(matches!(err("src_ip"), CompileError::Type(_)));
        assert!(matches!(err("payload"), CompileError::Type(_)));
        assert!(matches!(err("5"), CompileError::Type(_)));
        assert!(matches!(
            err("ip_in_cidr(src_port, \"10.0.0.0/8\")"),
            CompileError::Type(_)
        ));
    }

    #[test]
    fn syntax_errors_are_rejected() {
        assert!(matches!(err("src_port =="), CompileError::Syntax(_)));
        assert!(matches!(err("(src_port"), CompileError::Syntax(_)));
        assert!(matches!(err("src_port = 80"), CompileError::Syntax(_)));
        assert!(matches!(err("src_port & true"), CompileError::Syntax(_)));
        assert!(matches!(err("unknown_field == 1"), CompileError::Syntax(_)));
        assert!(matches!(err("len(payload)"), CompileError::Syntax(_)));
        assert!(matches!(
            err("payload.startsWith(\"a\")"),
            CompileError::Syntax(_)
        ));
        assert!(matches!(err(""), CompileError::Syntax(_)));
    }

    #[test]
    fn assignment_and_loops_do_not_exist() {
        // A single '=' lexes as an error (assignments are rejected).
        assert!(matches!(err("src_port = 1"), CompileError::Syntax(_)));
    }

    #[test]
    fn source_length_limit_is_enforced() {
        let long = "a".repeat(crate::MAX_SOURCE_LEN + 1);
        assert!(matches!(
            compile(&long),
            Err(CompileError::SourceTooLong(_))
        ));
    }

    #[test]
    fn depth_limit_is_enforced() {
        let deep = format!("{}1{}", "!".repeat(crate::MAX_AST_DEPTH + 1), "");
        assert!(matches!(compile(&deep), Err(CompileError::TooDeep)));
    }

    #[test]
    fn literal_cidr_is_validated_at_compile_time() {
        assert!(matches!(
            err("ip_in_cidr(remote_ip, \"banana\")"),
            CompileError::Type(_)
        ));
        ok("ip_in_cidr(remote_ip, \"fd00::/16\")");
    }

    #[test]
    fn byte_literal_escapes_roundtrip() {
        ok("payload.contains(b\"\\x00\\x01\\x7f\")");
    }
}
