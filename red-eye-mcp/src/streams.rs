use serde_json::{Value, json};
use url::Url;

use crate::backend::BackendClient;
use crate::config::MAX_STREAM_IDS;

// get_selected_streams: an operator copies a Red Eye UI selection as a
// link and hands it to an agent. The link is only parsed - its origin
// must equal the configured MCP_PUBLIC_URL origin, and the stream data
// comes from this service's own backend connection. Nothing is fetched
// from the link's origin.

#[derive(Debug)]
pub struct Link {
    pub ids: Vec<i64>,
}

/// Normalized "scheme://host[:port]" comparison key; default ports are
/// dropped so :443 and bare https compare equal.
fn origin_of(raw: &str) -> Result<String, &'static str> {
    let u = Url::parse(raw).map_err(|_| "malformed url")?;
    if u.scheme() != "http" && u.scheme() != "https" {
        return Err("only http and https are accepted");
    }
    if !u.username().is_empty() || u.password().is_some() {
        return Err("embedded credentials are not allowed");
    }
    let host = u
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or("missing host")?
        .to_ascii_lowercase();
    let scheme_default = if u.scheme() == "https" { 443 } else { 80 };
    match u.port() {
        Some(p) if p != scheme_default => Ok(format!("{}://{}:{}", u.scheme(), host, p)),
        _ => Ok(format!("{}://{}", u.scheme(), host)),
    }
}

/// Fail closed: without a parseable MCP_PUBLIC_URL no link can be
/// accepted, so the tool reports a configuration error instead.
pub fn expected_origin(public_url: Option<&str>) -> Result<String, String> {
    let raw = public_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "configuration error: MCP_PUBLIC_URL is not set; the operator must set it to the \
             Red Eye public origin (for example https://red-eye.example.com) so links can be \
             validated"
                .to_string()
        })?;
    origin_of(raw).map_err(|why| {
        format!(
            "configuration error: MCP_PUBLIC_URL is invalid ({}); set it to the Red Eye \
             public origin, for example https://red-eye.example.com",
            why
        )
    })
}

pub fn parse_link(args: &Value, public_origin: &str) -> Result<Link, String> {
    let raw = args
        .get("url")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or("invalid url: the url argument is missing or empty")?;
    let u = Url::parse(raw).map_err(|_| "invalid url: malformed url".to_string())?;
    if u.scheme() != "http" && u.scheme() != "https" {
        return Err("invalid url: only http and https links are accepted".to_string());
    }
    if !u.username().is_empty() || u.password().is_some() {
        return Err("invalid url: embedded credentials are not allowed".to_string());
    }
    let origin = origin_of(raw).map_err(|why| format!("invalid url: {}", why))?;
    if origin != public_origin {
        return Err(format!(
            "rejected url: origin '{}' is not the configured Red Eye origin ({}); \
             only links copied from the Red Eye UI are accepted",
            origin, public_origin
        ));
    }
    let ids = extract_ids(&u)?;
    Ok(Link { ids })
}

fn extract_ids(u: &Url) -> Result<Vec<i64>, String> {
    let mut multi = None;
    let mut single = None;
    for (k, v) in u.query_pairs() {
        if k == "streams" && multi.is_none() {
            multi = Some(v.into_owned());
        } else if k == "stream" && single.is_none() {
            single = Some(v.into_owned());
        }
    }
    let value = multi.or(single).ok_or_else(|| {
        "invalid url: the link carries no streams query parameter; copy it from the \
         Red Eye UI selection"
            .to_string()
    })?;
    parse_ids(&value)
}

fn parse_ids(value: &str) -> Result<Vec<i64>, String> {
    if value.trim().is_empty() {
        return Err("invalid streams param: no stream ids given".to_string());
    }
    let mut ids = Vec::new();
    for tok in value.split(',') {
        let t = tok.trim();
        let id: i64 = t.parse().map_err(|_| {
            format!(
                "invalid streams param: '{}' is not a stream id",
                t.chars().take(40).collect::<String>()
            )
        })?;
        if id <= 0 {
            return Err(format!(
                "invalid streams param: stream ids must be positive, got {}",
                id
            ));
        }
        ids.push(id);
    }
    if ids.len() > MAX_STREAM_IDS {
        return Err(format!(
            "rejected url: the link lists {} streams but this tool reads at most {} per \
             call; split the selection",
            ids.len(),
            MAX_STREAM_IDS
        ));
    }
    Ok(ids)
}

pub async fn run(
    args: &Value,
    backend: &BackendClient,
    public_url: Option<&str>,
) -> Result<Value, String> {
    let origin = expected_origin(public_url)?;
    let link = parse_link(args, &origin)?;
    let mut streams = Vec::with_capacity(link.ids.len());
    let mut failed_ids = Vec::new();
    for id in link.ids {
        match crate::tools::stream_evidence(backend, id).await {
            Ok(v) => streams.push(v),
            Err(e) => failed_ids.push(json!({"stream_id": id, "error": e})),
        }
    }
    Ok(json!({"streams": streams, "failed_ids": failed_ids}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Mode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const ORIGIN: &str = "https://red-eye.example.com";

    fn origin(public_url: Option<&str>) -> String {
        expected_origin(public_url).unwrap()
    }

    #[test]
    fn origin_config_errors_fail_closed() {
        let e = expected_origin(None).unwrap_err();
        assert!(e.contains("MCP_PUBLIC_URL"));
        assert!(
            expected_origin(Some(""))
                .unwrap_err()
                .contains("MCP_PUBLIC_URL")
        );
        assert!(
            expected_origin(Some("not a url"))
                .unwrap_err()
                .contains("MCP_PUBLIC_URL")
        );
        assert!(
            expected_origin(Some("ftp://red-eye.example.com"))
                .unwrap_err()
                .contains("MCP_PUBLIC_URL")
        );
    }

    #[test]
    fn origin_normalizes_scheme_host_port() {
        assert_eq!(
            origin(Some("https://Red-Eye.Example.com/")),
            "https://red-eye.example.com"
        );
        assert_eq!(
            origin(Some("https://red-eye.example.com:443/x")),
            "https://red-eye.example.com"
        );
        assert_eq!(
            origin(Some("http://red-eye.example.com:8080")),
            "http://red-eye.example.com:8080"
        );
    }

    #[test]
    fn link_origin_must_match_config() {
        let o = origin(Some(ORIGIN));
        assert!(
            parse_link(
                &json!({"url": "https://red-eye.example.com/packets?streams=1"}),
                &o
            )
            .is_ok()
        );
        assert!(
            parse_link(
                &json!({"url": "https://red-eye.example.com:443/packets?streams=1"}),
                &o
            )
            .is_ok()
        );
        assert!(
            parse_link(
                &json!({"url": "http://red-eye.example.com/packets?streams=1"}),
                &o
            )
            .is_err()
        );
        assert!(
            parse_link(
                &json!({"url": "https://evil.example.com/packets?streams=1"}),
                &o
            )
            .is_err()
        );
        assert!(
            parse_link(
                &json!({"url": "https://red-eye.example.com:8443/packets?streams=1"}),
                &o
            )
            .is_err()
        );
        assert!(
            parse_link(
                &json!({"url": "https://user@red-eye.example.com/p?streams=1"}),
                &o
            )
            .is_err()
        );
        assert!(parse_link(&json!({"url": "ftp://red-eye.example.com/p?streams=1"}), &o).is_err());
        assert!(parse_link(&json!({}), &o).is_err());
        assert!(parse_link(&json!({"url": "   "}), &o).is_err());
        assert!(parse_link(&json!({"url": "not a url"}), &o).is_err());
    }

    #[test]
    fn streams_param_parsing() {
        let o = origin(Some(ORIGIN));
        let ids = |u: &str| parse_link(&json!({"url": u}), &o).unwrap().ids;
        assert_eq!(
            ids("https://red-eye.example.com/packets?streams=1,2,3"),
            [1, 2, 3]
        );
        assert_eq!(ids("https://red-eye.example.com/packets?stream=42"), [42]);
        assert_eq!(
            ids("https://red-eye.example.com/packets?streams=15&tab=x"),
            [15]
        );
        assert_eq!(
            ids("https://red-eye.example.com/packets?streams= 7 , 8 "),
            [7, 8]
        );

        let bad = [
            "https://red-eye.example.com/packets",
            "https://red-eye.example.com/packets?streams=",
            "https://red-eye.example.com/packets?streams=1,,2",
            "https://red-eye.example.com/packets?streams=1,abc",
            "https://red-eye.example.com/packets?streams=0",
            "https://red-eye.example.com/packets?streams=-5",
            "https://red-eye.example.com/packets?streams=99999999999999999999",
        ];
        for u in bad {
            assert!(
                parse_link(&json!({"url": u}), &o).is_err(),
                "must reject {}",
                u
            );
        }
    }

    #[test]
    fn streams_param_is_capped() {
        let o = origin(Some(ORIGIN));
        let list = |n: usize| (1..=n).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        let at_cap = format!("{}/packets?streams={}", ORIGIN, list(MAX_STREAM_IDS));
        assert_eq!(
            parse_link(&json!({"url": at_cap}), &o).unwrap().ids.len(),
            MAX_STREAM_IDS
        );
        let over = format!("{}/packets?streams={}", ORIGIN, list(MAX_STREAM_IDS + 1));
        let err = parse_link(&json!({"url": over}), &o).unwrap_err();
        assert!(err.contains(&MAX_STREAM_IDS.to_string()), "err: {}", err);
    }

    fn backend(url: &str) -> BackendClient {
        BackendClient::new(&Config {
            backend_url: url.to_string(),
            service_token: "t".to_string(),
            mode: Mode::ReadOnly,
            bind: String::new(),
            public_url: None,
        })
        .unwrap()
    }

    // Minimal canned backend: 200 JSON for every id except 404.
    async fn stub_backend() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let req = String::from_utf8_lossy(&buf);
                let (status, body) = if req.contains("/stream/404/") {
                    ("404 Not Found", "{\"error\":\"not found\"}")
                } else if req.contains("/messages") {
                    ("200 OK", "{\"entries\":[],\"truncated\":false}")
                } else {
                    ("200 OK", "{\"id\":7,\"packet_count\":3}")
                };
                let resp = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    status,
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        format!("http://{}", addr)
    }

    #[tokio::test]
    async fn bundle_assembles_summary_and_messages_per_id() {
        let b = backend(&stub_backend().await);
        let out = run(
            &json!({"url": "https://red-eye.example.com/packets?streams=7,404"}),
            &b,
            Some(ORIGIN),
        )
        .await
        .unwrap();
        let streams = out["streams"].as_array().unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0]["stream_id"], json!(7));
        assert_eq!(streams[0]["summary"]["id"], json!(7));
        assert!(streams[0]["messages"].is_object());
        let failed = out["failed_ids"].as_array().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["stream_id"], json!(404));
        assert!(failed[0]["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn backend_failure_lands_in_failed_ids_not_a_global_error() {
        let b = backend("http://127.0.0.1:1");
        let out = run(
            &json!({"url": "https://red-eye.example.com/packets?streams=5,6"}),
            &b,
            Some(ORIGIN),
        )
        .await
        .unwrap();
        assert!(out["streams"].as_array().unwrap().is_empty());
        assert_eq!(out["failed_ids"].as_array().unwrap().len(), 2);
    }
}
