//! Tokenizer for the expression DSL. Byte-oriented: sources are ASCII
//! operators around UTF-8 string/byte literals.

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    Uint(u64),
    /// Double-quoted UTF-8 string literal, validated at parse time.
    Str(Vec<u8>),
    /// b"..." byte literal with \xNN and simple escapes.
    Bytes(Vec<u8>),
    LParen,
    RParen,
    Comma,
    Dot,
    AndAnd,
    OrOr,
    Not,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Ident(s) => write!(f, "'{s}'"),
            Token::Uint(n) => write!(f, "'{n}'"),
            Token::Str(_) => write!(f, "string literal"),
            Token::Bytes(_) => write!(f, "byte literal"),
            Token::LParen => write!(f, "'('"),
            Token::RParen => write!(f, "')'"),
            Token::Comma => write!(f, "','"),
            Token::Dot => write!(f, "'.'"),
            Token::AndAnd => write!(f, "'&&'"),
            Token::OrOr => write!(f, "'||'"),
            Token::Not => write!(f, "'!'"),
            Token::Eq => write!(f, "'=='"),
            Token::Neq => write!(f, "'!='"),
            Token::Lt => write!(f, "'<'"),
            Token::Lte => write!(f, "'<='"),
            Token::Gt => write!(f, "'>'"),
            Token::Gte => write!(f, "'>='"),
            Token::Eof => write!(f, "end of expression"),
        }
    }
}

#[derive(Debug)]
pub struct LexError {
    pub message: String,
    pub offset: usize,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lex error at byte {}: {}", self.offset, self.message)
    }
}

pub fn tokenize(source: &str) -> Result<Vec<Token>, LexError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            b'&' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'&' {
                    tokens.push(Token::AndAnd);
                    i += 2;
                } else {
                    return Err(LexError {
                        message: "single '&' is not supported (use '&&')".into(),
                        offset: i,
                    });
                }
            }
            b'|' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'|' {
                    tokens.push(Token::OrOr);
                    i += 2;
                } else {
                    return Err(LexError {
                        message: "single '|' is not supported (use '||')".into(),
                        offset: i,
                    });
                }
            }
            b'!' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Neq);
                    i += 2;
                } else {
                    tokens.push(Token::Not);
                    i += 1;
                }
            }
            b'=' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Eq);
                    i += 2;
                } else {
                    return Err(LexError {
                        message: "single '=' is an assignment and is not supported (use '==')"
                            .into(),
                        offset: i,
                    });
                }
            }
            b'<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Lte);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Gte);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            b'"' => {
                let (s, next) = lex_string(bytes, i)?;
                tokens.push(Token::Str(s));
                i = next;
            }
            b'b' if i + 1 < bytes.len() && bytes[i + 1] == b'"' => {
                let (s, next) = lex_string(bytes, i + 1)?;
                tokens.push(Token::Bytes(s));
                i = next;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                // An identifier must not start with a digit; digits here
                // always form a number (no floats in the DSL).
                let text = std::str::from_utf8(&bytes[start..i]).unwrap_or("0");
                let n: u64 = text.parse().map_err(|_| LexError {
                    message: "number out of u64 range".into(),
                    offset: start,
                })?;
                tokens.push(Token::Uint(n));
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(Token::Ident(
                    std::str::from_utf8(&bytes[start..i])
                        .unwrap_or("")
                        .to_string(),
                ));
            }
            other => {
                return Err(LexError {
                    message: format!("unexpected character '{}'", other as char),
                    offset: i,
                });
            }
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

/// Lex a double-quoted string starting at `start` (the quote byte).
fn lex_string(bytes: &[u8], start: usize) -> Result<(Vec<u8>, usize), LexError> {
    let mut out = Vec::new();
    let mut i = start + 1;
    loop {
        let b = *bytes.get(i).ok_or_else(|| LexError {
            message: "unterminated string literal".into(),
            offset: start,
        })?;
        match b {
            b'"' => return Ok((out, i + 1)),
            b'\\' => {
                let esc = *bytes.get(i + 1).ok_or_else(|| LexError {
                    message: "unterminated escape sequence".into(),
                    offset: i,
                })?;
                i += 2;
                match esc {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'n' => out.push(b'\n'),
                    b't' => out.push(b'\t'),
                    b'r' => out.push(b'\r'),
                    b'0' => out.push(0),
                    b'x' => {
                        let h1 = *bytes.get(i).ok_or_else(|| LexError {
                            message: "truncated \\x escape".into(),
                            offset: i,
                        })?;
                        let h2 = *bytes.get(i + 1).ok_or_else(|| LexError {
                            message: "truncated \\x escape".into(),
                            offset: i,
                        })?;
                        let hex_byte = |c: u8| -> Result<u8, LexError> {
                            (c as char)
                                .to_digit(16)
                                .map(|d| d as u8)
                                .ok_or_else(|| LexError {
                                    message: "invalid hex digit in \\x escape".into(),
                                    offset: i,
                                })
                        };
                        out.push(hex_byte(h1)? * 16 + hex_byte(h2)?);
                        i += 2;
                    }
                    other => {
                        return Err(LexError {
                            message: format!("unsupported escape '\\{}'", other as char),
                            offset: i,
                        });
                    }
                }
            }
            _ => {
                out.push(b);
                i += 1;
            }
        }
    }
}

/// Split a Str token's raw bytes into a Rust string, rejecting invalid
/// UTF-8 for string literals (byte literals may hold arbitrary bytes).
pub fn str_token_to_string(raw: &[u8], offset: usize) -> Result<String, LexError> {
    String::from_utf8(raw.to_vec()).map_err(|_| LexError {
        message: "string literal is not valid UTF-8 (use b\"...\" for bytes)".into(),
        offset,
    })
}
