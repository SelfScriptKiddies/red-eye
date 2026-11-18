//! Bounded CEL-style expression DSL for packet filters.
//!
//! One compiled expression evaluates one packet. Expressions must return a
//! boolean, cannot assign or loop, and have no host capabilities: the only
//! inputs are the fields of a single packet and the single helper
//! `ip_in_cidr`. The same crate compiles and evaluates for the analyzer and
//! the agent, so accepted syntax and results cannot drift.
//!
//! Limits (enforced, documented): source length, AST depth, evaluation
//! steps, and inspected payload bytes. Any evaluation error or limit
//! exhaustion is a NON-MATCH for that filter at the call site; other
//! filters and packets continue.

pub mod eval;
pub mod fixtures;
pub mod ip;
pub mod lexer;
pub mod parser;

pub use eval::{eval, EvalError, EvalInput};
pub use parser::{compile, CompileError, Expr};

/// Maximum expression source length in characters.
pub const MAX_SOURCE_LEN: usize = 4096;

/// Maximum AST depth (parenthesized nesting plus operator chains).
pub const MAX_AST_DEPTH: usize = 32;

/// Maximum evaluation steps per expression per packet. A step is one AST
/// node visit; a full expression stays far below this, so exhaustion means
/// a pathological or hostile source.
pub const MAX_EVAL_STEPS: usize = 1000;

/// Only the first N bytes of the payload are searchable by `contains`.
/// Longer payloads still evaluate (matches beyond the limit are out of
/// scope by definition); the limit bounds per-packet scan cost.
pub const MAX_INSPECTED_PAYLOAD_BYTES: usize = 65536;
