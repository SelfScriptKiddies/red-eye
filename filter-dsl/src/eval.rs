//! Bounded evaluator. Zero allocation: values are borrowed from the AST or
//! the packet input, never constructed.

use crate::parser::{CmpOp, Expr, FieldId, Node};
use std::fmt;

/// All inputs describe ONE packet. String fields are the rendered forms
/// (IPs as text, protocol upper-case, direction in|out).
pub struct EvalInput<'a> {
    pub payload: &'a [u8],
    pub src_ip: &'a str,
    pub dst_ip: &'a str,
    pub src_port: u64,
    pub dst_port: u64,
    pub local_ip: &'a str,
    pub remote_ip: &'a str,
    pub local_port: u64,
    pub remote_port: u64,
    /// "TCP" or "UDP"
    pub protocol: &'a str,
    /// "in" or "out"
    pub direction: &'a str,
    pub service_id: u64,
    pub agent_id: &'a str,
    /// Epoch seconds.
    pub timestamp: u64,
}

#[derive(Debug)]
pub enum EvalError {
    StepLimitExceeded,
    InvalidIp,
    InvalidCidr,
    /// A runtime type misuse the static checker could not see (does not
    /// exist today; kept so a future bug fails closed, not open).
    TypeMismatch,
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvalError::StepLimitExceeded => write!(f, "evaluation step limit exceeded"),
            EvalError::InvalidIp => write!(f, "ip_in_cidr argument is not a valid IP address"),
            EvalError::InvalidCidr => write!(f, "ip_in_cidr argument is not a valid CIDR"),
            EvalError::TypeMismatch => write!(f, "runtime type mismatch"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Value<'a> {
    Bool(bool),
    Uint(u64),
    Str(&'a str),
    Bytes(&'a [u8]),
}

/// Evaluate the expression against one packet. `Err` means the filter's
/// result is unknown: callers must treat it as NON-MATCH for this filter
/// only and count the error.
pub fn eval(expr: &Expr, input: &EvalInput<'_>) -> Result<bool, EvalError> {
    let mut steps = 0usize;
    let value = eval_node(&expr.root, input, &mut steps)?;
    match value {
        Value::Bool(b) => Ok(b),
        _ => Err(EvalError::TypeMismatch),
    }
}

fn eval_node<'a>(
    node: &'a Node,
    input: &'a EvalInput<'a>,
    steps: &mut usize,
) -> Result<Value<'a>, EvalError> {
    *steps += 1;
    if *steps > crate::MAX_EVAL_STEPS {
        return Err(EvalError::StepLimitExceeded);
    }

    Ok(match node {
        Node::Bool(b) => Value::Bool(*b),
        Node::Uint(n) => Value::Uint(*n),
        Node::Str(s) => Value::Str(s.as_str()),
        Node::Bytes(b) => Value::Bytes(b.as_slice()),
        Node::Field(id) => field_value(*id, input),
        Node::Not(inner) => {
            let v = eval_node(inner, input, steps)?;
            match v {
                Value::Bool(b) => Value::Bool(!b),
                _ => return Err(EvalError::TypeMismatch),
            }
        }
        Node::And(a, b) => {
            let va = eval_node(a, input, steps)?;
            let vb = eval_node(b, input, steps)?;
            match (va, vb) {
                (Value::Bool(x), Value::Bool(y)) => Value::Bool(x && y),
                _ => return Err(EvalError::TypeMismatch),
            }
        }
        Node::Or(a, b) => {
            let va = eval_node(a, input, steps)?;
            let vb = eval_node(b, input, steps)?;
            match (va, vb) {
                (Value::Bool(x), Value::Bool(y)) => Value::Bool(x || y),
                _ => return Err(EvalError::TypeMismatch),
            }
        }
        Node::Cmp(op, a, b) => {
            let va = eval_node(a, input, steps)?;
            let vb = eval_node(b, input, steps)?;
            Value::Bool(compare(*op, va, vb)?)
        }
        Node::IpInCidr(ip_node, cidr_node) => {
            let ip_val = eval_node(ip_node, input, steps)?;
            let cidr_val = eval_node(cidr_node, input, steps)?;
            let (Value::Str(ip), Value::Str(cidr)) = (ip_val, cidr_val) else {
                return Err(EvalError::TypeMismatch);
            };
            let parsed_ip = crate::ip::parse_ip(ip).ok_or(EvalError::InvalidIp)?;
            let parsed_cidr = crate::ip::parse_cidr(cidr).ok_or(EvalError::InvalidCidr)?;
            Value::Bool(crate::ip::cidr_contains(&parsed_cidr, &parsed_ip))
        }
        Node::Contains(hay, needle) => {
            let hay_val = eval_node(hay, input, steps)?;
            let needle_val = eval_node(needle, input, steps)?;
            match (hay_val, needle_val) {
                (Value::Bytes(h), Value::Bytes(n)) => Value::Bool(bounded_contains(h, n)),
                (Value::Str(h), Value::Str(n)) => Value::Bool(h.contains(n)),
                _ => return Err(EvalError::TypeMismatch),
            }
        }
    })
}

/// Sub-slice containment over raw bytes.
fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        // An empty needle matches by convention in str::contains; keep the
        // same behavior for bytes.
        return needle.is_empty();
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Payload containment inspects at most MAX_INSPECTED_PAYLOAD_BYTES bytes
/// of the haystack: needles that only match beyond the limit do not match
/// (the documented scan bound).
fn bounded_contains(haystack: &[u8], needle: &[u8]) -> bool {
    let limit = crate::MAX_INSPECTED_PAYLOAD_BYTES;
    if haystack.len() > limit {
        bytes_contains(&haystack[..limit], needle)
    } else {
        bytes_contains(haystack, needle)
    }
}

fn field_value<'a>(id: FieldId, input: &'a EvalInput<'a>) -> Value<'a> {
    match id {
        FieldId::Payload => Value::Bytes(input.payload),
        FieldId::SrcIp => Value::Str(input.src_ip),
        FieldId::DstIp => Value::Str(input.dst_ip),
        FieldId::SrcPort => Value::Uint(input.src_port),
        FieldId::DstPort => Value::Uint(input.dst_port),
        FieldId::LocalIp => Value::Str(input.local_ip),
        FieldId::RemoteIp => Value::Str(input.remote_ip),
        FieldId::LocalPort => Value::Uint(input.local_port),
        FieldId::RemotePort => Value::Uint(input.remote_port),
        FieldId::Protocol => Value::Str(input.protocol),
        FieldId::Direction => Value::Str(input.direction),
        FieldId::ServiceId => Value::Uint(input.service_id),
        FieldId::AgentId => Value::Str(input.agent_id),
        FieldId::Timestamp => Value::Uint(input.timestamp),
    }
}

fn compare(op: CmpOp, a: Value<'_>, b: Value<'_>) -> Result<bool, EvalError> {
    let mismatch = || EvalError::TypeMismatch;
    match (a, b) {
        (Value::Uint(x), Value::Uint(y)) => Ok(match op {
            CmpOp::Eq => x == y,
            CmpOp::Neq => x != y,
            CmpOp::Lt => x < y,
            CmpOp::Lte => x <= y,
            CmpOp::Gt => x > y,
            CmpOp::Gte => x >= y,
        }),
        (Value::Str(x), Value::Str(y)) => Ok(match op {
            CmpOp::Eq => x == y,
            CmpOp::Neq => x != y,
            CmpOp::Lt => x < y,
            CmpOp::Lte => x <= y,
            CmpOp::Gt => x > y,
            CmpOp::Gte => x >= y,
        }),
        (Value::Bytes(x), Value::Bytes(y)) => Ok(match op {
            CmpOp::Eq => x == y,
            CmpOp::Neq => x != y,
            _ => return Err(mismatch()),
        }),
        (Value::Bool(x), Value::Bool(y)) => Ok(match op {
            CmpOp::Eq => x == y,
            CmpOp::Neq => x != y,
            _ => return Err(mismatch()),
        }),
        _ => Err(mismatch()),
    }
}
