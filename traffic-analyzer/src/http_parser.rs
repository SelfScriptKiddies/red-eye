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

/// Try to parse payload as HTTP/1.x request or response.
/// Returns None if the payload doesn't look like HTTP.
pub fn try_parse_http(payload: &[u8]) -> Option<HttpParsed> {
    if payload.is_empty() {
        return None;
    }

    // Quick check: does it look like HTTP?
    if payload.starts_with(b"HTTP/") {
        return parse_response(payload);
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
        return parse_request(payload);
    }

    None
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
