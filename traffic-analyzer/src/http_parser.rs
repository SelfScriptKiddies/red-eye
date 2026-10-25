use log::debug;

/// Parsed HTTP message (request or response)
pub struct HttpParsed {
    pub is_request: bool,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status_code: Option<u16>,
    pub http_version: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body_offset: usize,
}

/// Try to parse payload as HTTP/1.x or HTTP/2 (text-serialized) request or response.
/// Returns None if the payload doesn't look like HTTP.
pub fn try_parse_http(payload: &[u8]) -> Option<HttpParsed> {
    if payload.is_empty() {
        return None;
    }

    // Harpoon serializes HTTP/2 as "HTTP/2" — httparse only understands HTTP/1.x
    // Rewrite to HTTP/1.1 for parsing, but remember the original for body_offset
    let original = payload;
    let (payload, is_h2) =
        if payload.starts_with(b"HTTP/2 ") || payload.windows(8).any(|w| w == b" HTTP/2\r") {
            let mut fixed = payload.to_vec();
            // Replace "HTTP/2 " with "HTTP/1.1 " in responses
            if let Some(pos) = fixed.windows(7).position(|w| w == b"HTTP/2 ") {
                fixed.splice(pos..pos + 6, b"HTTP/1.1".iter().cloned());
            }
            // Replace " HTTP/2\r" with " HTTP/1.1\r" in requests
            if let Some(pos) = fixed.windows(8).position(|w| w == b" HTTP/2\r") {
                fixed.splice(pos + 1..pos + 7, b"HTTP/1.1".iter().cloned());
            }
            (std::borrow::Cow::Owned(fixed), true)
        } else {
            (std::borrow::Cow::Borrowed(payload), false)
        };
    let payload = payload.as_ref();

    // Quick check: does it look like HTTP?
    if payload.starts_with(b"HTTP/") {
        let mut parsed = parse_response(payload)?;
        if is_h2 {
            parsed.http_version = Some("2".to_string());
            // Fix body_offset: rewrite shifted it by 2 bytes
            fix_body_offset_for_h2(&mut parsed, original);
        }
        return Some(parsed);
    }

    // Check for common HTTP methods
    if payload.starts_with(b"GET ")
        || payload.starts_with(b"POST ")
        || payload.starts_with(b"PUT ")
        || payload.starts_with(b"DELETE ")
        || payload.starts_with(b"PATCH ")
        || payload.starts_with(b"HEAD ")
        || payload.starts_with(b"OPTIONS ")
        || payload.starts_with(b"CONNECT ")
        || payload.starts_with(b"TRACE ")
    {
        let mut parsed = parse_request(payload)?;
        if is_h2 {
            parsed.http_version = Some("2".to_string());
            fix_body_offset_for_h2(&mut parsed, original);
        }
        return Some(parsed);
    }

    None
}

/// The HTTP/2→HTTP/1.1 rewrite adds 2 bytes, making httparse's body_offset
/// incorrect for the original payload. Recalculate from the original bytes.
fn fix_body_offset_for_h2(parsed: &mut HttpParsed, original: &[u8]) {
    if let Some(pos) = original.windows(4).position(|w| w == b"\r\n\r\n") {
        parsed.body_offset = pos + 4;
    }
}

fn parse_request(payload: &[u8]) -> Option<HttpParsed> {
    let mut headers_buf = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers_buf);

    match req.parse(payload) {
        Ok(httparse::Status::Complete(body_offset)) => {
            let version = req.version.map(|v| format!("1.{}", v));
            let headers: Vec<(String, String)> = req
                .headers
                .iter()
                .filter(|h| h.name != httparse::EMPTY_HEADER.name)
                .map(|h| {
                    (
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    )
                })
                .collect();

            debug!(
                "Parsed HTTP request: {} {} ({} headers)",
                req.method.unwrap_or("?"),
                req.path.unwrap_or("?"),
                headers.len()
            );

            Some(HttpParsed {
                is_request: true,
                method: req.method.map(|m| m.to_string()),
                path: req.path.map(|p| p.to_string()),
                status_code: None,
                http_version: version,
                headers,
                body_offset,
            })
        }
        Ok(httparse::Status::Partial) => {
            // Incomplete headers — still extract what we can
            let version = req.version.map(|v| format!("1.{}", v));
            let headers: Vec<(String, String)> = req
                .headers
                .iter()
                .filter(|h| h.name != httparse::EMPTY_HEADER.name)
                .map(|h| {
                    (
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    )
                })
                .collect();

            if req.method.is_some() {
                Some(HttpParsed {
                    is_request: true,
                    method: req.method.map(|m| m.to_string()),
                    path: req.path.map(|p| p.to_string()),
                    status_code: None,
                    http_version: version,
                    headers,
                    body_offset: payload.len(),
                })
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

fn parse_response(payload: &[u8]) -> Option<HttpParsed> {
    let mut headers_buf = [httparse::EMPTY_HEADER; 64];
    let mut res = httparse::Response::new(&mut headers_buf);

    match res.parse(payload) {
        Ok(httparse::Status::Complete(body_offset)) => {
            let version = res.version.map(|v| format!("1.{}", v));
            let headers: Vec<(String, String)> = res
                .headers
                .iter()
                .filter(|h| h.name != httparse::EMPTY_HEADER.name)
                .map(|h| {
                    (
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    )
                })
                .collect();

            debug!(
                "Parsed HTTP response: {} ({} headers)",
                res.code.unwrap_or(0),
                headers.len()
            );

            Some(HttpParsed {
                is_request: false,
                method: None,
                path: None,
                status_code: res.code,
                http_version: version,
                headers,
                body_offset,
            })
        }
        Ok(httparse::Status::Partial) => {
            let version = res.version.map(|v| format!("1.{}", v));
            let headers: Vec<(String, String)> = res
                .headers
                .iter()
                .filter(|h| h.name != httparse::EMPTY_HEADER.name)
                .map(|h| {
                    (
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    )
                })
                .collect();

            if res.code.is_some() {
                Some(HttpParsed {
                    is_request: false,
                    method: None,
                    path: None,
                    status_code: res.code,
                    http_version: version,
                    headers,
                    body_offset: payload.len(),
                })
            } else {
                None
            }
        }
        Err(_) => None,
    }
}
