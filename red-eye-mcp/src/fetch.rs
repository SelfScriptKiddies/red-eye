use std::fmt;
use std::io::Cursor;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::config::{
    FETCH_MAX_BODY_BYTES, FETCH_TIMEOUT_SECS, MAX_OUTPUT_CHARS, MAX_REDIRECTS, MAX_URL_LEN,
};
use html2text::render::{PlainDecorator, TextDecorator};
use serde_json::{Value, json};
use url::{Host, Url};

// fetch_url: bounded HTTP GET of a public operator-provided URL, rendered
// as LLM-readable text. This is the only network egress the service makes
// outside its backend connection, so it is disabled unless the deployment
// opts in (MCP_ENABLE_FETCH_URL) and every hop passes the address policy.
// The connection goes to an address validated BEFORE the request via a
// per-request pinned client, so DNS cannot flip the target between the
// check and the connect.

/// Rendered link form produced by the inline decorator.
const HTML_RENDER_WIDTH: usize = 100;
/// Metadata extraction never scans beyond this prefix of the document.
const HEAD_SCAN_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub enum FetchError {
    InvalidUrl(&'static str),
    PolicyDenied,
    ResolveFailed,
    TooManyRedirects,
    BodyTooLarge { bytes: u64 },
    Timeout,
    Http(u16),
    Io(String),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::InvalidUrl(why) => write!(f, "invalid url: {}", why),
            FetchError::PolicyDenied => write!(
                f,
                "request refused: the address policy only fetches hosts on the public \
                 internet; loopback, private, link-local and other special-purpose \
                 ranges are blocked (SSRF protection)"
            ),
            FetchError::ResolveFailed => write!(f, "dns resolution failed for the target host"),
            FetchError::TooManyRedirects => {
                write!(f, "too many redirects (limit {})", MAX_REDIRECTS)
            }
            FetchError::BodyTooLarge { bytes } => write!(
                f,
                "response body exceeded the {} MiB fetch limit (observed {} bytes)",
                FETCH_MAX_BODY_BYTES / (1024 * 1024),
                bytes
            ),
            FetchError::Timeout => write!(
                f,
                "fetch exceeded the {}s wall-clock budget",
                FETCH_TIMEOUT_SECS
            ),
            FetchError::Http(status) => write!(f, "target returned http status {}", status),
            FetchError::Io(m) => write!(f, "network error: {}", m),
        }
    }
}

/// Pure allowlist core of the address policy. Public unicast addresses only;
/// every other family is denied so a single denied resolver answer refuses
/// the whole request.
pub fn address_allowed(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_documentation()
                || (o[0] == 198 && matches!(o[1], 18 | 19))
                || (o[0] == 100 && (64..=127).contains(&o[1])))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return address_allowed(IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (s[0] & 0xffc0) == 0xfe80
                || (s[0] & 0xfe00) == 0xfc00)
        }
    }
}

fn validate_url(raw: &str) -> Result<Url, FetchError> {
    if raw.len() > MAX_URL_LEN {
        return Err(FetchError::InvalidUrl("url exceeds the length limit"));
    }
    let url = Url::parse(raw).map_err(|_| FetchError::InvalidUrl("malformed url"))?;
    // The url crate keeps no trace of an empty userinfo ("http://@host"),
    // so the authority portion of the raw input is checked directly.
    let authority = raw
        .split("://")
        .nth(1)
        .and_then(|a| a.split(['/', '?', '#']).next())
        .unwrap_or("");
    if authority.contains('@') {
        return Err(FetchError::InvalidUrl(
            "embedded credentials are not allowed",
        ));
    }
    validate_target(&url)?;
    Ok(url)
}

/// Scheme, credential and host policy; literal-IP hosts are judged here so
/// redirect hops are refused before any DNS or connection work.
fn validate_target(url: &Url) -> Result<(), FetchError> {
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(FetchError::InvalidUrl("only http and https are allowed"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(FetchError::InvalidUrl(
            "embedded credentials are not allowed",
        ));
    }
    match url.host() {
        Some(Host::Domain(d)) if !d.trim().is_empty() => Ok(()),
        Some(Host::Ipv4(ip)) if address_allowed(IpAddr::V4(ip)) => Ok(()),
        Some(Host::Ipv6(ip)) if address_allowed(IpAddr::V6(ip)) => Ok(()),
        Some(_) => Err(FetchError::PolicyDenied),
        None => Err(FetchError::InvalidUrl("missing host")),
    }
}

/// Resolve the Location value of one redirect against the current URL and
/// re-run the full target validation on it.
fn redirect_target(current: &Url, location: &str) -> Result<Url, FetchError> {
    let joined = current
        .join(location)
        .map_err(|_| FetchError::InvalidUrl("unusable redirect location"))?;
    validate_target(&joined)?;
    Ok(joined)
}

/// The redirect budget: at most MAX_REDIRECTS hops are followed.
fn follow_allowed(redirects_done: usize) -> Result<(), FetchError> {
    if redirects_done >= MAX_REDIRECTS {
        Err(FetchError::TooManyRedirects)
    } else {
        Ok(())
    }
}

/// Host (no brackets, lowercased) plus the validated address the connection
/// must use. Host is None for literal-IP urls, which bypass DNS entirely.
struct Pinned {
    host: Option<String>,
    addr: SocketAddr,
}

async fn resolve_and_pin(url: &Url) -> Result<Pinned, FetchError> {
    let port = url.port_or_known_default().unwrap_or(80);
    match url.host() {
        Some(Host::Ipv4(ip)) => {
            if !address_allowed(IpAddr::V4(ip)) {
                return Err(FetchError::PolicyDenied);
            }
            Ok(Pinned {
                host: None,
                addr: SocketAddr::new(IpAddr::V4(ip), port),
            })
        }
        Some(Host::Ipv6(ip)) => {
            if !address_allowed(IpAddr::V6(ip)) {
                return Err(FetchError::PolicyDenied);
            }
            Ok(Pinned {
                host: None,
                addr: SocketAddr::new(IpAddr::V6(ip), port),
            })
        }
        Some(Host::Domain(d)) => {
            let host = d.to_lowercase();
            let target = (host.clone(), port);
            let addrs = tokio::task::spawn_blocking(move || target.to_socket_addrs())
                .await
                .map_err(|_| FetchError::Io("resolver task failed".to_string()))?
                .map_err(|_| FetchError::ResolveFailed)?;
            let mut addrs: Vec<SocketAddr> = addrs.collect();
            if addrs.is_empty() {
                return Err(FetchError::ResolveFailed);
            }
            // One denied answer denies the request: no fallback to a
            // "safe-looking" address from a hostile mixed RRset.
            if addrs.iter().any(|a| !address_allowed(a.ip())) {
                return Err(FetchError::PolicyDenied);
            }
            addrs.sort();
            Ok(Pinned {
                host: Some(host),
                addr: addrs[0],
            })
        }
        None => Err(FetchError::InvalidUrl("missing host")),
    }
}

fn pinned_client(pinned: &Pinned) -> Result<reqwest::Client, FetchError> {
    let mut b = reqwest::Client::builder()
        // Redirects are followed manually so every hop re-validates.
        .redirect(reqwest::redirect::Policy::none())
        // Env proxies would connect from the proxy and resolve DNS there,
        // bypassing the pin; egress goes direct to the validated address.
        .no_proxy()
        .connect_timeout(Duration::from_secs(5));
    if let Some(host) = &pinned.host {
        // Port 0: the request URL's port always wins; the override only
        // replaces DNS resolution (keyed by bare host, brackets stripped).
        b = b.resolve(host, SocketAddr::new(pinned.addr.ip(), 0));
    }
    b.build()
        .map_err(|_| FetchError::Io("client init failed".to_string()))
}

pub struct FetchOutcome {
    pub final_url: String,
    pub content_type: String,
    pub bytes: u64,
    pub title: String,
    pub text: String,
}

/// Tool entry point: arguments in, LLM-readable JSON result out.
pub async fn run(args: &Value) -> Result<Value, FetchError> {
    let raw = args
        .get("url")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or(FetchError::InvalidUrl("missing url argument"))?;
    let outcome = tokio::time::timeout(Duration::from_secs(FETCH_TIMEOUT_SECS), fetch_inner(raw))
        .await
        .map_err(|_| FetchError::Timeout)??;
    Ok(json!({
        "final_url": outcome.final_url,
        "content_type": outcome.content_type,
        "bytes": outcome.bytes,
        "title": outcome.title,
        "content": outcome.text,
    }))
}

async fn fetch_inner(raw: &str) -> Result<FetchOutcome, FetchError> {
    let mut current = validate_url(raw)?;
    for hops in 0..=MAX_REDIRECTS {
        let pinned = resolve_and_pin(&current).await?;
        let client = pinned_client(&pinned)?;
        let mut resp = client
            .get(current.as_str())
            .send()
            .await
            .map_err(|e| FetchError::Io(e.without_url().to_string()))?;
        let status = resp.status();
        if status.is_redirection() {
            follow_allowed(hops)?;
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or(FetchError::Http(status.as_u16()))?
                .to_string();
            current = redirect_target(&current, &location)?;
            continue;
        }
        if !status.is_success() {
            return Err(FetchError::Http(status.as_u16()));
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let mut bytes = Vec::new();
        let mut total: u64 = 0;
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| FetchError::Io(e.without_url().to_string()))?
        {
            total += chunk.len() as u64;
            if total > FETCH_MAX_BODY_BYTES as u64 {
                return Err(FetchError::BodyTooLarge { bytes: total });
            }
            bytes.extend_from_slice(&chunk);
        }
        return Ok(render_content(
            current.as_str().to_string(),
            &content_type,
            &bytes,
        ));
    }
    Err(FetchError::TooManyRedirects)
}

fn render_content(final_url: String, content_type: &str, body: &[u8]) -> FetchOutcome {
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let (title, text) = if mime == "text/html" || mime == "application/xhtml+xml" {
        let (title, description) = scan_head(body);
        let mut text = String::new();
        if !description.is_empty() {
            text.push_str("Description: ");
            text.push_str(&description);
            text.push_str("\n\n");
        }
        text.push_str(&extract_html_text(body));
        (title, text)
    } else if is_textual(&mime) {
        (String::new(), String::from_utf8_lossy(body).into_owned())
    } else {
        (
            String::new(),
            format!(
                "binary content ({}, {} bytes) is not fetchable via this tool",
                if mime.is_empty() { "unknown" } else { &mime },
                body.len()
            ),
        )
    };
    FetchOutcome {
        final_url,
        content_type: if content_type.is_empty() {
            mime
        } else {
            content_type.to_string()
        },
        bytes: body.len() as u64,
        title,
        text: cap_chars(text),
    }
}

/// Textual families passed through as-is: any text/*, JSON and XML
/// including vendor suffixes.
fn is_textual(mime: &str) -> bool {
    mime.starts_with("text/")
        || mime == "application/json"
        || mime == "application/xml"
        || mime.ends_with("+json")
        || mime.ends_with("+xml")
}

fn cap_chars(s: String) -> String {
    if s.chars().count() <= MAX_OUTPUT_CHARS {
        return s;
    }
    let mut out: String = s.chars().take(MAX_OUTPUT_CHARS).collect();
    out.push_str(&format!(
        "\n\n[truncated: output capped at {} characters; the remainder of the document was not included]",
        MAX_OUTPUT_CHARS
    ));
    out
}

/// Title and meta description parsed from the raw prefix because the
/// renderer drops <head> entirely.
fn scan_head(body: &[u8]) -> (String, String) {
    let prefix = String::from_utf8_lossy(&body[..body.len().min(HEAD_SCAN_BYTES)]);
    (find_title(&prefix), find_meta_description(&prefix))
}

fn ascii_find(hay: &str, needle: &str, from: usize) -> Option<usize> {
    let h = hay.as_bytes();
    let n = needle.as_bytes();
    if h.len() < n.len() {
        return None;
    }
    (from..=h.len() - n.len()).find(|&i| &h[i..i + n.len()] == n)
}

fn ascii_find_ci(hay: &str, needle: &str, from: usize) -> Option<usize> {
    let h = hay.as_bytes();
    let n = needle.as_bytes();
    if h.len() < n.len() {
        return None;
    }
    (from..=h.len() - n.len()).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

fn find_title(prefix: &str) -> String {
    let open = match ascii_find_ci(prefix, "<title", 0) {
        Some(i) => i,
        None => return String::new(),
    };
    let gt = match ascii_find(prefix, ">", open) {
        Some(i) => i + 1,
        None => return String::new(),
    };
    let close = match ascii_find_ci(prefix, "</title", gt) {
        Some(i) => i,
        None => return String::new(),
    };
    decode_entities(prefix[gt..close].trim())
        .chars()
        .take(512)
        .collect()
}

fn find_meta_description(prefix: &str) -> String {
    let mut from = 0;
    while let Some(start) = ascii_find_ci(prefix, "<meta", from) {
        let end = ascii_find(prefix, ">", start)
            .unwrap_or(start + 200)
            .min(prefix.len());
        let tag = &prefix[start..end];
        let is_desc = ascii_find_ci(tag, "name", 0).is_some()
            && ascii_find_ci(tag, "description", 0).is_some();
        if let Some(v) = is_desc
            .then(|| tag_attribute_value(tag, "content"))
            .flatten()
        {
            return decode_entities(v.trim()).chars().take(1024).collect();
        }
        from = start + 5;
    }
    String::new()
}

fn tag_attribute_value(tag: &str, attr: &str) -> Option<String> {
    let key_ci = ascii_find_ci(tag, attr, 0)?;
    let after_key = &tag[key_ci + attr.len()..];
    let eq = ascii_find(after_key, "=", 0)?;
    let rest = after_key[eq + 1..].trim_start();
    let first = rest.chars().next()?;
    if first == '"' || first == '\'' {
        let body = &rest[1..];
        let end = ascii_find(body, &first.to_string(), 0)?;
        Some(body[..end].to_string())
    } else {
        // Bare (unquoted) attribute value ends at whitespace or tag close.
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '>' || c == '/')
            .unwrap_or(rest.len());
        Some(rest[..end].to_string())
    }
}

fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
}

/// html2text skips script/style/head content natively; the custom
/// decorator changes link form from "[text][n]" plus footnotes to the
/// inline "[text] (url)" the tool contract promises.
struct InlineLinkDecorator {
    inner: PlainDecorator,
    link_url: String,
}

impl InlineLinkDecorator {
    fn new() -> Self {
        InlineLinkDecorator {
            inner: PlainDecorator::new(),
            link_url: String::new(),
        }
    }
}

impl TextDecorator for InlineLinkDecorator {
    type Annotation = ();

    fn decorate_link_start(&mut self, url: &str) -> (String, ()) {
        self.link_url = url.to_string();
        ("[".to_string(), ())
    }

    fn decorate_link_end(&mut self) -> String {
        format!("] ({})", self.link_url)
    }

    fn decorate_em_start(&self) -> (String, ()) {
        self.inner.decorate_em_start()
    }

    fn decorate_em_end(&self) -> String {
        self.inner.decorate_em_end()
    }

    fn decorate_strong_start(&self) -> (String, ()) {
        self.inner.decorate_strong_start()
    }

    fn decorate_strong_end(&self) -> String {
        self.inner.decorate_strong_end()
    }

    fn decorate_strikeout_start(&self) -> (String, ()) {
        self.inner.decorate_strikeout_start()
    }

    fn decorate_strikeout_end(&self) -> String {
        self.inner.decorate_strikeout_end()
    }

    fn decorate_code_start(&self) -> (String, ()) {
        self.inner.decorate_code_start()
    }

    fn decorate_code_end(&self) -> String {
        self.inner.decorate_code_end()
    }

    fn decorate_preformat_first(&self) {
        self.inner.decorate_preformat_first()
    }

    fn decorate_preformat_cont(&self) {
        self.inner.decorate_preformat_cont()
    }

    fn decorate_image(&mut self, src: &str, title: &str) -> (String, ()) {
        self.inner.decorate_image(src, title)
    }

    fn header_prefix(&self, level: usize) -> String {
        self.inner.header_prefix(level)
    }

    fn quote_prefix(&self) -> String {
        self.inner.quote_prefix()
    }

    fn unordered_item_prefix(&self) -> String {
        self.inner.unordered_item_prefix()
    }

    fn ordered_item_prefix(&self, i: i64) -> String {
        self.inner.ordered_item_prefix(i)
    }

    fn make_subblock_decorator(&self) -> Self {
        InlineLinkDecorator {
            inner: self.inner.clone(),
            link_url: String::new(),
        }
    }
}

fn extract_html_text(body: &[u8]) -> String {
    html2text::from_read_with_decorator(
        Cursor::new(body),
        HTML_RENDER_WIDTH,
        InlineLinkDecorator::new(),
    )
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    #[test]
    fn address_policy_denies_every_special_range() {
        let denied = [
            "127.0.0.1",
            "127.254.1.2",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "0.0.0.0",
            "255.255.255.255",
            "224.0.0.1",
            "192.0.2.1",
            "::1",
            "::",
            "fe80::1",
            "febf::1",
            "fc00::1",
            "fd12:3456::1",
            "ff02::1",
        ];
        for ip in denied {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(!address_allowed(ip), "must deny {}", ip);
        }
    }

    #[test]
    fn address_policy_allows_public_internet() {
        let allowed = [
            "8.8.8.8",
            "1.1.1.1",
            "2606:4700::1111",
            "172.32.0.1",
            "100.128.0.1",
        ];
        for ip in allowed {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(address_allowed(ip), "must allow {}", ip);
        }
        let mapped = IpAddr::V6("::ffff:10.0.0.1".parse::<Ipv6Addr>().unwrap());
        assert!(!address_allowed(mapped));
    }

    #[test]
    fn url_validation_rejects_bad_inputs() {
        for raw in [
            "ftp://example.com/x",
            "file:///etc/passwd",
            "http://user:pass@example.com/",
            "http://@example.com/",
            "",
            "not a url",
            "http://",
            "http:///",
            &format!("https://example.com/{}", "a".repeat(MAX_URL_LEN)),
        ] {
            assert!(
                validate_url(raw).is_err(),
                "must reject {:?}",
                if raw.len() > 40 { &raw[..40] } else { raw }
            );
        }
        assert!(validate_url("https://example.com/x").is_ok());
        assert!(validate_url("http://example.com:8080/x").is_ok());
    }

    #[test]
    fn literal_private_ip_target_is_refused() {
        assert!(matches!(
            validate_url("http://192.168.1.5/x"),
            Err(FetchError::PolicyDenied)
        ));
        assert!(matches!(
            validate_url("http://[fd00::1]/"),
            Err(FetchError::PolicyDenied)
        ));
        assert!(validate_url("http://8.8.8.8/dns-query").is_ok());
    }

    #[test]
    fn redirect_target_revalidates_each_hop() {
        let start = validate_url("https://public.example/redirect").unwrap();
        let next = redirect_target(&start, "/doc?a=1").unwrap();
        assert_eq!(next.as_str(), "https://public.example/doc?a=1");
        let hops = redirect_target(&start, "https://other.example/a").unwrap();
        assert_eq!(hops.host_str(), Some("other.example"));
        assert!(matches!(
            redirect_target(&start, "http://10.0.0.5/internal"),
            Err(FetchError::PolicyDenied)
        ));
        assert!(matches!(
            redirect_target(&start, "ftp://public.example/x"),
            Err(FetchError::InvalidUrl(_))
        ));
        assert!(matches!(
            redirect_target(&start, "http://user@public.example/"),
            Err(FetchError::InvalidUrl(_))
        ));
    }

    #[test]
    fn redirect_budget_is_capped() {
        for done in 0..MAX_REDIRECTS {
            assert!(follow_allowed(done).is_ok());
        }
        assert!(matches!(
            follow_allowed(MAX_REDIRECTS),
            Err(FetchError::TooManyRedirects)
        ));
    }

    #[test]
    fn html_extraction_yields_title_links_and_drops_scripts() {
        let html = b"<html><head><title>Alert page</title>\
            <meta name=\"description\" content=\"A test page\">\
            <style>.x { color: red } SECRET-STYLE</style>\
            <script>SECRET-SCRIPT</script></head>\
            <body><h1>Heading</h1><p>See <a href=\"https://ex.example/doc\">the doc</a>.</p></body></html>";
        let out = render_content(
            "https://public.example/".into(),
            "text/html; charset=utf-8",
            html,
        );
        assert_eq!(out.title, "Alert page");
        assert!(out.text.contains("Heading"));
        assert!(out.text.contains("[the doc] (https://ex.example/doc)"));
        assert!(!out.text.contains("SECRET-SCRIPT"));
        assert!(!out.text.contains("SECRET-STYLE"));
        assert!(!out.text.contains("[1]:"));
        assert!(out.content_type.contains("text/html"));
    }

    #[test]
    fn oversized_html_output_is_capped_with_marker() {
        let mut html = String::from("<html><body>");
        for _ in 0..1000 {
            html.push_str("<p>lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor</p>");
        }
        html.push_str("</body></html>");
        let out = render_content(
            "https://public.example/big".into(),
            "text/html",
            html.as_bytes(),
        );
        assert!(out.text.chars().count() > MAX_OUTPUT_CHARS);
        assert!(out.text.contains(&format!(
            "[truncated: output capped at {} characters",
            MAX_OUTPUT_CHARS
        )));
    }

    #[test]
    fn json_and_text_pass_through_capped() {
        let body = br#"{"flag":"CTF{a}","note":"ok"}"#;
        let out = render_content(
            "https://public.example/api".into(),
            "application/json",
            body,
        );
        assert!(out.text.contains("CTF{a}"));
        assert_eq!(out.bytes, body.len() as u64);

        let vendor = render_content(
            "https://public.example/api".into(),
            "application/vnd.api+json; charset=utf-8",
            body,
        );
        assert!(vendor.text.contains("CTF{a}"));
    }

    #[test]
    fn binary_content_types_never_leak_payload() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let out = render_content("https://public.example/img".into(), "image/png", &payload);
        assert!(out.text.contains("image/png"));
        assert!(out.text.contains("not fetchable"));
        assert!(!out.text.contains('\u{0}'));
        assert_eq!(out.bytes, 4096);
    }

    #[test]
    fn entity_and_bare_meta_forms_parse() {
        let html = b"<html><head><title>A &amp; B</title>\
            <meta content='quoted desc' name='description'></head><body>x</body></html>";
        let (title, desc) = scan_head(html);
        assert_eq!(title, "A & B");
        assert_eq!(desc, "quoted desc");
        let html2 = b"<html><head><meta name=description content=bare></head><body>y</body></html>";
        let (_, desc2) = scan_head(html2);
        assert_eq!(desc2, "bare");
    }

    #[test]
    fn errors_display_sanitized_and_typed() {
        let e = FetchError::BodyTooLarge { bytes: 2_100_000 };
        assert!(e.to_string().contains("2100000"));
        assert!(!FetchError::PolicyDenied.to_string().contains("127.0.0.1"));
    }
}
