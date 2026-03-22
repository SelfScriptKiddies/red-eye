use log::debug;

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
/// Returns None if not a valid gRPC body.
pub fn try_parse_grpc_body(
    body: &[u8],
    path: Option<&str>,
    is_request: bool,
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

    // Decode protobuf without .proto — raw field dump
    let decoded_json = decode_protobuf_raw(payload);

    debug!("gRPC parsed: {}/{} ({} bytes payload)",
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

/// Decode protobuf without .proto — extract raw fields with wire types.
/// Returns JSON-like string with field numbers and values.
fn decode_protobuf_raw(data: &[u8]) -> String {
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

        match wire_type {
            0 => {
                // Varint
                if let Some((value, new_pos)) = decode_varint(data, pos) {
                    fields.push(format!("\"field_{}\": {}", field_number, value));
                    pos = new_pos;
                } else {
                    break;
                }
            }
            1 => {
                // 64-bit
                if pos + 8 <= data.len() {
                    let value = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    fields.push(format!("\"field_{}\": {}", field_number, value));
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
                        // Try as UTF-8 string first
                        if let Ok(s) = std::str::from_utf8(value) {
                            if s.chars().all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t') {
                                fields.push(format!("\"field_{}\": \"{}\"", field_number, s.replace('\"', "\\\"")));
                            } else {
                                fields.push(format!("\"field_{}\": \"<{} bytes>\"", field_number, length));
                            }
                        } else {
                            fields.push(format!("\"field_{}\": \"<{} bytes>\"", field_number, length));
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
                    fields.push(format!("\"field_{}\": {}", field_number, value));
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
