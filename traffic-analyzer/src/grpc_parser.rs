use log::debug;
use std::collections::HashMap;

use crate::proto_schema::ProtoSchema;

/// Parsed gRPC message
pub struct GrpcParsed {
    pub service_name: Option<String>,
    pub method_name: Option<String>,
    /// Decoded body as JSON (if proto available) or raw field dump
    pub decoded_json: String,
    pub is_request: bool,
}

/// Try to parse a gRPC message from HTTP body.
/// gRPC body format: [1 byte compressed flag] [4 bytes length BE] [protobuf payload]
/// If `proto` is provided, field names are resolved from the .proto definition.
/// Returns None if not a valid gRPC body.
pub fn try_parse_grpc_body(
    body: &[u8],
    path: Option<&str>,
    is_request: bool,
    proto: Option<&ProtoSchema>,
) -> Option<GrpcParsed> {
    if body.len() < 5 {
        return None;
    }

    let _compressed = body[0];
    let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;

    if body.len() < 5 + length {
        return None;
    }

    let payload = &body[5..5 + length];

    // Extract service/method from path: /package.Service/Method
    let (service_name, method_name) = if let Some(p) = path {
        let parts: Vec<&str> = p.trim_start_matches('/').splitn(2, '/').collect();
        if parts.len() == 2 {
            (Some(parts[0].to_string()), Some(parts[1].to_string()))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Resolve field names from .proto if available
    let field_names = proto.and_then(|p| {
        let svc = service_name.as_deref()?;
        let method = method_name.as_deref()?;
        p.resolve_fields(svc, method, is_request)
    });

    let decoded_json = decode_protobuf_raw(payload, field_names);

    debug!(
        "gRPC parsed: {}/{} ({} bytes payload)",
        service_name.as_deref().unwrap_or("?"),
        method_name.as_deref().unwrap_or("?"),
        length
    );

    Some(GrpcParsed {
        service_name,
        method_name,
        decoded_json,
        is_request,
    })
}

/// Decode protobuf — extract fields with wire types.
/// If `field_names` is provided, uses real names; otherwise falls back to field_N.
fn decode_protobuf_raw(data: &[u8], field_names: Option<&HashMap<u64, String>>) -> String {
    let mut fields = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (tag, new_pos) = match decode_varint(data, pos) {
            Some(v) => v,
            None => break,
        };

        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        pos = new_pos;

        let fname = field_names
            .and_then(|m| m.get(&field_number))
            .map(|s| s.as_str());
        let key = match fname {
            Some(name) => format!("\"{}\"", name),
            None => format!("\"field_{}\"", field_number),
        };

        match wire_type {
            0 => {
                // Varint
                if let Some((value, new_pos)) = decode_varint(data, pos) {
                    fields.push(format!("{}: {}", key, value));
                    pos = new_pos;
                } else {
                    break;
                }
            }
            1 => {
                // 64-bit
                if pos + 8 <= data.len() {
                    let value = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    fields.push(format!("{}: {}", key, value));
                    pos += 8;
                } else {
                    break;
                }
            }
            2 => {
                // Length-delimited (string, bytes, embedded message)
                if let Some((length, new_pos)) = decode_varint(data, pos) {
                    let length = length as usize;
                    pos = new_pos;
                    if pos + length <= data.len() {
                        let value = &data[pos..pos + length];
                        if let Ok(s) = std::str::from_utf8(value) {
                            if s.chars()
                                .all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t')
                            {
                                fields.push(format!("{}: \"{}\"", key, s.replace('\"', "\\\"")));
                            } else {
                                fields.push(format!("{}: \"<{} bytes>\"", key, length));
                            }
                        } else {
                            fields.push(format!("{}: \"<{} bytes>\"", key, length));
                        }
                        pos += length;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            5 => {
                // 32-bit
                if pos + 4 <= data.len() {
                    let value = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    fields.push(format!("{}: {}", key, value));
                    pos += 4;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    format!("{{{}}}", fields.join(", "))
}

fn decode_varint(data: &[u8], start: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = start;

    while pos < data.len() {
        let byte = data[pos];
        result |= ((byte & 0x7F) as u64) << shift;
        pos += 1;
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}
